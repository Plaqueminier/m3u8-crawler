import {
  S3Client,
  DeleteObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
} from "@aws-sdk/client-s3";
import * as dotenv from "dotenv";
import { DatabaseSync } from "node:sqlite";

dotenv.config();

// Configure folders to delete - add folder names here
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
  size: number;
}

function formatFileSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`;
}

async function deleteFile(bucketName: string, key: string): Promise<void> {
  try {
    await s3Client.send(
      new DeleteObjectCommand({
        Bucket: bucketName,
        Key: key,
      })
    );
    console.log(`Deleted: ${key}`);
  } catch (error) {
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

      continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    console.log(`Deleted preview folder: previews/${previewKey}/`);
  } catch (error) {
    console.error(`Failed to delete preview folder ${previewKey}:`, error);
    throw error;
  }
}

async function deleteFilesByFolder(): Promise<void> {
  if (TO_DELETE.length === 0) {
    console.log("No folders specified in TO_DELETE array. Please add folder names and run again.");
    return;
  }

  const bucketName = process.env.R2_BUCKET || "";
  let deletedFiles = 0;
  let deletedPreviews = 0;

  // Open database connection using built-in SQLite
  const db = new DatabaseSync("../videos.db");

  try {
    // Build the WHERE clause for folder matching
    const folderConditions = TO_DELETE.map(() => "key LIKE ? || '/%'").join(" OR ");
    const folderParams = TO_DELETE.map(folder => folder);

    // Get list of files to delete from specified folders (non-favorites only)
    const query = `
      SELECT 
        key,
        SUBSTR(key, 1, INSTR(key, '.') - 1) as previewKey,
        size
      FROM videos 
      WHERE (${folderConditions})
        AND key NOT LIKE 'previews/%'
        AND favorite = 0
    `;

    const stmt = db.prepare(query);
    const rawResults = stmt.all(...folderParams);
    const filesToDelete: FileToDelete[] = rawResults.map(row => ({
      key: row.key as string,
      previewKey: row.previewKey as string,
      size: (row.size as number) || 0
    }));

    // Calculate total size in bytes
    const totalSizeBytes = filesToDelete.reduce((sum, file) => sum + file.size, 0);

    console.log(`Found ${filesToDelete.length} files to delete`);
    console.log(`From folders: ${TO_DELETE.join(", ")}`);
    console.log(`💾 Total storage to be freed: ${formatFileSize(totalSizeBytes)}`);
    console.log("(Only non-favorite files will be deleted)");

    if (filesToDelete.length === 0) {
      console.log("No files found to delete.");
      return;
    }

    // Confirm before proceeding
    console.log("\nWARNING: This will permanently delete these files!");
    console.log("Press Ctrl+C within 10 seconds to abort...");
    await new Promise((resolve) => setTimeout(resolve, 10000));

    // Prepare delete statement
    const deleteStmt = db.prepare("DELETE FROM videos WHERE key = ?");

    for (const file of filesToDelete) {
      try {
        // Delete main file
        await deleteFile(bucketName, file.key);
        deletedFiles++;

        // Delete preview folder
        await deletePreviewFolder(bucketName, file.previewKey);
        deletedPreviews++;

        // Update database
        deleteStmt.run(file.key);
      } catch (error) {
        // Log error but continue with next file
        console.error(`Error processing ${file.key}:`, error);
      }

      // Progress update every 10 files
      if (deletedFiles % 10 === 0) {
        console.log(
          `Progress: ${deletedFiles}/${filesToDelete.length} files processed`
        );
      }
    }

    // Calculate actual freed space (only for successfully deleted files)
    const actualFreedBytes = filesToDelete.slice(0, deletedFiles).reduce((sum, file) => sum + file.size, 0);

    console.log("\nDeletion complete!");
    console.log(`Total files deleted: ${deletedFiles}`);
    console.log(`Total preview folders deleted: ${deletedPreviews}`);
    console.log(`💾 Storage freed: ${formatFileSize(actualFreedBytes)}`);
  } catch (error) {
    console.error("Error during deletion:", error);
    throw error;
  } finally {
    db.close();
  }
}

// Run the deletion
console.log("Starting folder-based deletion process...");
deleteFilesByFolder().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});