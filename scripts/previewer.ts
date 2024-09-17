import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { promisify } from "util";
import { exec } from "child_process";
import logger from "../src/logger";
import { Readable } from "stream";

dotenv.config();

const execAsync = promisify(exec);

// Load environment variables
const REGION = "auto";
const ENDPOINT = process.env.R2_ENDPOINT ?? "";
const ACCESS_KEY_ID = process.env.R2_ACCESS_KEY ?? "";
const SECRET_ACCESS_KEY = process.env.R2_SECRET_KEY ?? "";
const BUCKET_NAME = process.env.R2_BUCKET ?? "";

// Configure the S3 client to use Cloudflare R2
const s3Client = new S3Client({
  region: REGION,
  endpoint: ENDPOINT,
  credentials: {
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
  },
});

async function listVideos(): Promise<string[]> {
  const videoKeys: string[] = [];
  let continuationToken: string | undefined;

  do {
    const command = new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      ContinuationToken: continuationToken,
    });

    const response = await s3Client.send(command);
    const contents = response.Contents || [];

    for (const object of contents) {
      if (
        object.Key &&
        object.Key.toLowerCase().endsWith(".mp4") &&
        !object.Key.startsWith("previews/")
      ) {
        videoKeys.push(object.Key);
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return videoKeys;
}

async function checkPreviewExists(videoKey: string): Promise<boolean> {
  const previewKey = `previews/${path.basename(
    videoKey,
    ".mp4"
  )}/segment_01.mp4`;
  try {
    await s3Client.send(
      new HeadObjectCommand({
        Bucket: BUCKET_NAME,
        Key: previewKey,
      })
    );
    return true;
  } catch {
    logger.info(`No preview for ${videoKey}:`);
    return false;
  }
}

async function downloadFile(key: string, outputPath: string): Promise<void> {
  const headCommand = new HeadObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
  });

  const headResponse = await s3Client.send(headCommand);
  const totalSize = headResponse.ContentLength ?? 0;

  const command = new GetObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
  });

  const { Body } = await s3Client.send(command);
  const writer = fs.createWriteStream(outputPath);

  let downloadedBytes = 0;
  let lastLoggedPercent = 0;

  return new Promise((resolve, reject) => {
    if (Body instanceof Readable) {
      Body.on("data", (chunk: Buffer) => {
        downloadedBytes += chunk.length;
        const percent = Math.round((downloadedBytes / totalSize) * 100);

        // Log progress every 5%
        if (percent >= lastLoggedPercent + 5) {
          logger.info(`Downloading ${key}: ${percent}% complete`);
          lastLoggedPercent = percent;
        }
      });

      Body.pipe(writer);
      writer.on("finish", resolve);
      writer.on("error", reject);
    }
  });
}

async function uploadFile(filePath: string, key: string): Promise<void> {
  const fileStream = fs.createReadStream(filePath);

  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET_NAME,
      Key: key,
      Body: fileStream,
      ContentType: "video/mp4",
    },
  });

  try {
    await upload.done();
    fileStream.close();
    logger.info(`Uploaded ${key} successfully`);
  } catch (err) {
    logger.error("Error uploading file:", err);
  }
}

async function processVideo(inputPath: string): Promise<string[]> {
  const outputFiles: string[] = [];
  const segmentDuration = 30;
  const numSegments = 10;

  logger.info("Analyzing video duration...");
  const { stdout } = await execAsync(
    `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`
  );
  const duration = parseFloat(stdout.trim());
  logger.info(`Video duration: ${duration.toFixed(2)} seconds`);

  for (let i = 1; i <= numSegments; i++) {
    const outputFile = `segment_${i.toString().padStart(2, "0")}.mp4`;
    outputFiles.push(outputFile);

    let ffmpegCommand: string;

    if (i === 1) {
      ffmpegCommand = `ffmpeg -i "${inputPath}" -t ${segmentDuration} -vf "scale='min(854,iw)':min'(480,ih)':force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2" -c:v libx264 -crf 23 -preset medium -c:a aac -b:a 128k "${outputFile}"`;
    } else if (i === numSegments) {
      ffmpegCommand = `ffmpeg -sseof -${segmentDuration} -i "${inputPath}" -vf "scale='min(854,iw)':min'(480,ih)':force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2" -c:v libx264 -crf 23 -preset medium -c:a aac -b:a 128k "${outputFile}"`;
    } else {
      const startTime = (duration * (i - 1)) / 9 - segmentDuration / 2;
      ffmpegCommand = `ffmpeg -ss ${startTime.toFixed(
        2
      )} -i "${inputPath}" -t ${segmentDuration} -vf "scale='min(854,iw)':min'(480,ih)':force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2" -c:v libx264 -crf 23 -preset medium -c:a aac -b:a 128k "${outputFile}"`;
    }

    logger.info(`Processing segment ${i}/${numSegments}...`);
    try {
      const { stderr } = await execAsync(ffmpegCommand);
      if (stderr) {
        logger.info(`ffmpeg output for segment ${i}:`, stderr);
      }
      logger.info(`Segment ${i}/${numSegments} processed successfully.`);
    } catch (error) {
      logger.error(`Error processing segment ${i}:`, error);
      throw error; // Re-throw to be caught in the main processing loop
    }
  }

  return outputFiles;
}

async function processAllVideos(): Promise<void> {
  try {
    const videoKeys = await listVideos();
    logger.info(`Found ${videoKeys.length} videos in total.`);

    for (const videoKey of videoKeys) {
      logger.info(`Checking video: ${videoKey}`);

      // Check if preview exists just before processing
      const previewExists = await checkPreviewExists(videoKey);
      if (previewExists) {
        logger.info(`Skipping ${videoKey}: Preview already exists`);
        continue;
      }

      logger.info(`Processing video: ${videoKey}`);
      const localPath = `temp_${path.basename(videoKey)}`;

      try {
        // Download the video from R2
        logger.info(`Starting download of ${videoKey}...`);
        await downloadFile(videoKey, localPath);
        logger.info(`Download of ${videoKey} complete.`);

        // Process the video
        logger.info("Processing video...");
        const segmentFiles = await processVideo(localPath);

        // Upload segments back to R2
        logger.info("Uploading segments...");
        for (const segmentFile of segmentFiles) {
          const segmentKey = `previews/${path.basename(
            videoKey,
            ".mp4"
          )}/${segmentFile}`;
          await uploadFile(segmentFile, segmentKey);
          fs.unlinkSync(segmentFile); // Delete local segment file after upload
        }

        // Clean up
        fs.unlinkSync(localPath);
        logger.info(`Finished processing ${videoKey}`);
      } catch (error) {
        logger.error(`Error processing ${videoKey}:`, error);
      }
    }
    logger.info("All videos processed successfully!");
    process.exit(0);
  } catch (error) {
    logger.error("An error occurred while processing videos:", error);
    process.exit(1);
  }
}

processAllVideos();
