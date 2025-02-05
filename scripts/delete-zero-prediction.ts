import {
  S3Client,
  DeleteObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
} from "@aws-sdk/client-s3";
import * as dotenv from "dotenv";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

dotenv.config();

const DO_NOT_DELETE = [];

const TO_DELETE = [];

const s3Client = new S3Client({
  region: "auto",
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY || "",
    secretAccessKey: process.env.R2_SECRET_KEY || "",
  },
});

interface FileToDelete {
  key: string;
  previewKey: string;
}

async function deleteFile(bucketName: string, key: string): Promise<void> {
  try {
    await s3Client.send(
      new DeleteObjectCommand({
        Bucket: bucketName,
        Key: key,
      })
    );
    // eslint-disable-next-line no-console
    console.log(`Deleted: ${key}`);
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error(`Failed to delete ${key}:`, error);
    throw error;
  }
}

async function deletePreviewFolder(
  bucketName: string,
  previewKey: string
): Promise<void> {
  let continuationToken: string | undefined;

  try {
    do {
      const command = new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: `previews/${previewKey}/`,
        ContinuationToken: continuationToken,
      });

      const response: ListObjectsV2CommandOutput = await s3Client.send(command);

      if (!response.Contents || response.Contents.length === 0) {
        break;
      }

      // Delete all files in the preview folder
      for (const object of response.Contents) {
        if (object.Key) {
          await deleteFile(bucketName, object.Key);
        }
      }

      continuationToken = (response as ListObjectsV2CommandOutput)
        .NextContinuationToken;
    } while (continuationToken);

    // eslint-disable-next-line no-console
    console.log(`Deleted preview folder: previews/${previewKey}/`);
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error(`Failed to delete preview folder ${previewKey}:`, error);
    throw error;
  }
}

async function deleteZeroPredictionFiles(): Promise<void> {
  const bucketName = process.env.R2_BUCKET || "";
  let deletedFiles = 0;
  let deletedPreviews = 0;

  // Open database connection
  const db = await open({
    filename: "../videos.db",
    driver: sqlite3.Database,
  });

  try {
    // Get list of files to delete (zero prediction + TO_DELETE folders)
    const filesToDelete = await db.all<FileToDelete[]>(
      `SELECT 
        key,
        SUBSTR(key, 1, INSTR(key, '.') - 1) as previewKey
      FROM videos 
      WHERE (
        (
          LENGTH(prediction) - LENGTH(REPLACE(prediction, '1', '')) < 10
          AND key NOT LIKE 'previews/%'
          AND favorite = 0
          AND ${DO_NOT_DELETE.map(
            (folder) => `key NOT LIKE '${folder}/%'`
          ).join(" AND ")}
        )
        OR (
          ${TO_DELETE.map((folder) => `key LIKE '${folder}/%'`).join(" OR ")}
        )
        AND favorite = 0
      )`
    );

    // eslint-disable-next-line no-console
    console.log(`Found ${filesToDelete.length} files to delete`);
    // eslint-disable-next-line no-console
    console.log("Including:");
    // eslint-disable-next-line no-console
    console.log("- Files with prediction < 10");
    // eslint-disable-next-line no-console
    console.log(
      `- All non-favorite files from folders: ${TO_DELETE.join(", ")}`
    );

    // Confirm before proceeding
    // eslint-disable-next-line no-console
    console.log("\nWARNING: This will permanently delete these files!");
    // eslint-disable-next-line no-console
    console.log("Press Ctrl+C within 10 seconds to abort...");
    await new Promise((resolve) => setTimeout(resolve, 10000));

    for (const file of filesToDelete) {
      try {
        // Delete main file
        await deleteFile(bucketName, file.key);
        deletedFiles++;

        // Delete preview folder
        await deletePreviewFolder(bucketName, file.previewKey);
        deletedPreviews++;

        // Update database
        await db.run("DELETE FROM videos WHERE key = ?", file.key);
      } catch (error) {
        // Log error but continue with next file
        // eslint-disable-next-line no-console
        console.error(`Error processing ${file.key}:`, error);
      }

      // Progress update every 10 files
      if (deletedFiles % 10 === 0) {
        // eslint-disable-next-line no-console
        console.log(
          `Progress: ${deletedFiles}/${filesToDelete.length} files processed`
        );
      }
    }

    // eslint-disable-next-line no-console
    console.log("\nDeletion complete!");
    // eslint-disable-next-line no-console
    console.log(`Total files deleted: ${deletedFiles}`);
    // eslint-disable-next-line no-console
    console.log(`Total preview folders deleted: ${deletedPreviews}`);
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error("Error during deletion:", error);
    throw error;
  } finally {
    await db.close();
  }
}

// Run the deletion
// eslint-disable-next-line no-console
console.log("Starting deletion process...");
deleteZeroPredictionFiles().catch((error) => {
  // eslint-disable-next-line no-console
  console.error("Fatal error:", error);
  process.exit(1);
});
