import logger from "./logger";
import fs from "fs";
import { S3Client } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
import { Upload } from "@aws-sdk/lib-storage";
import path from "path";
import { processVideo } from "./preview";
import { open } from "sqlite";
import sqlite3 from "sqlite3";
dotenv.config();

// Load environment variables
const REGION = "auto"; // Cloudflare R2 uses 'auto' as the region
const ENDPOINT = process.env.R2_ENDPOINT ?? "";
const ACCESS_KEY_ID = process.env.R2_ACCESS_KEY ?? "";
const SECRET_ACCESS_KEY = process.env.R2_SECRET_KEY ?? "";
const BUCKET_NAME = process.env.R2_BUCKET ?? ""; // Replace with your actual bucket name

// Configure the S3 client to use Cloudflare R2
const s3Client = new S3Client({
  region: REGION,
  endpoint: ENDPOINT,
  credentials: {
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
  },
});

async function uploadFileInDb(name: string, fileName: string, size: number): Promise<void> {
  const db = await open({
    filename: process.env.DATABASE_PATH!,
    driver: sqlite3.Database,
  });

  const response = await db.run(
    "INSERT INTO videos (name, key, size, lastModified) VALUES (?, ?, ?, ?)",
    [name, fileName, size, new Date()]
  );
  logger.info("File uploaded in db", { metadata: response });
}

async function uploadFile(filePath: string): Promise<void> {
  const fileStream = fs.createReadStream(filePath);
  const fileName = `${path.basename(filePath).slice(0, -44)}/${path.basename(
    filePath
  )}`;

  // Create an upload object from AWS SDK's lib-storage
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET_NAME,
      Key: fileName,
      Body: fileStream,
      ContentType: "video/mp4", // Default content type
    },
  });

  upload.on("httpUploadProgress", (progress) => {
    logger.info(
      `Uploaded ${progress.loaded} bytes out of ${
        progress.total
      } bytes. (${Math.round(
        ((progress.loaded ?? 1) / (progress.total ?? 1)) * 100
      )}%)`
    );
  });

  try {
    logger.info("Starting file upload...", {
      metadata: {
        fileName,
        username: fileName.slice(0, fileName.indexOf("/")),
      },
    });
    const response = await upload.done();
    const userName = fileName.slice(0, fileName.indexOf("/"));
    logger.info("File uploaded successfully:", {
      metadata: {
        fileName,
        username: fileName.slice(0, fileName.indexOf("/")),
        response,
      },
    });
    fileStream.close();
    await uploadFileInDb(userName, fileName, fileStream.bytesRead);
    await processVideo(fileName);
    process.exit();
  } catch (err) {
    logger.error("Error uploading file:", { metadata: err });
    process.exit(1);
  }
}

const filePath = process.argv[2] ?? "";
uploadFile(filePath);
