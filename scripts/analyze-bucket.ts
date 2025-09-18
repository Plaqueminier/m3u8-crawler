import {
  S3Client,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
} from "@aws-sdk/client-s3";
import * as dotenv from "dotenv";
import * as fs from "fs/promises";
import * as path from "path";
import { DatabaseSync } from "node:sqlite";

const DO_NOT_DELETE: string[] = [];

const TO_DELETE: string[] = [];

dotenv.config();

interface MonthlyStats {
  fileCount: number;
  totalSize: number;
  averageSize: number;
}

interface MonthlyStatsMap {
  [monthKey: string]: MonthlyStats;
}

interface PreviewStats {
  totalSize: number;
  fileCount: number;
  previewFolders: string[];
}

interface DatabaseStats {
  totalSeen: number;
  totalFavorites: number;
  lastSeenDate: Date | null;
  averagePrediction: number;
  averageFavoritePrediction: number;
}

interface FolderStats {
  fileCount: number;
  totalSize: number;
  lastModified: Date | null;
  averageSize: number;
  monthlyStats: MonthlyStatsMap;
  previewStats: PreviewStats;
  dbStats: DatabaseStats;
}

interface FolderStatsMap {
  [key: string]: FolderStats;
}

interface KeyToFolderMap {
  [key: string]: string;
}

const formatBytes = (bytes: number): string => {
  if (bytes === 0) {
    return "0 Bytes";
  }
  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
};

const formatDate = (date: Date | null): string => {
  if (!date) {
    return "Never";
  }
  return date.toLocaleString();
};

const getMonthKey = (date: Date): string => {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(
    2,
    "0"
  )}`;
};

const extractKeyFromPath = (filePath: string): string => {
  const fileName = path.basename(filePath);
  return fileName.split(".")[0]; // Assumes key is everything before the first dot
};

const s3Client = new S3Client({
  region: "auto",
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY || "",
    secretAccessKey: process.env.R2_SECRET_KEY || "",
  },
});

async function analyzeBucket(): Promise<void> {
  const bucketName = process.env.R2_BUCKET || "";
  const folderStats: FolderStatsMap = {};
  const keyToFolderMap: KeyToFolderMap = {};
  let continuationToken: string | undefined;
  let hasReachedPreviews = false;
  let totalFiles = 0;

  // Open database connection
  const db = new DatabaseSync("../videos.db");

  try {
    // eslint-disable-next-line no-console
    console.log("Starting main bucket analysis...");

    // First pass: Analyze all non-preview folders and build key mapping
    do {
      const command = new ListObjectsV2Command({
        Bucket: bucketName,
        StartAfter: hasReachedPreviews ? "previewszz" : undefined,
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
      });

      const response = await s3Client.send(command);

      // eslint-disable-next-line no-console
      console.log(
        `Processing batch of ${response.Contents?.length || 0} files...`
      );

      if (!response.Contents || response.Contents.length === 0) {
        // eslint-disable-next-line no-console
        console.log("No more files to process");
        break;
      }

      for (const object of response.Contents) {
        if (!object.Key) {
          continue;
        }

        if (++totalFiles % 1000 === 0) {
          // eslint-disable-next-line no-console
          console.log(`Processed ${totalFiles} files so far...`);
        }

        if (!hasReachedPreviews && object.Key.startsWith("previews/")) {
          hasReachedPreviews = true;
          // eslint-disable-next-line no-console
          console.log("Found previews folder, skipping to previewszz...");
          break;
        }

        if (!object.Key.startsWith("previews/")) {
          const folderPath = path.dirname(object.Key);
          const size = object.Size || 0;
          const lastModified = object.LastModified || null;
          const key = extractKeyFromPath(object.Key);

          // Map this key to its parent folder
          keyToFolderMap[key] = folderPath;

          if (!folderStats[folderPath]) {
            folderStats[folderPath] = {
              fileCount: 0,
              totalSize: 0,
              lastModified: null,
              averageSize: 0,
              monthlyStats: {},
              previewStats: {
                totalSize: 0,
                fileCount: 0,
                previewFolders: [],
              },
              dbStats: {
                totalSeen: 0,
                totalFavorites: 0,
                lastSeenDate: null,
                averagePrediction: 0,
                averageFavoritePrediction: 0,
              },
            };
          }

          folderStats[folderPath].fileCount++;
          folderStats[folderPath].totalSize += size;

          const currentLastModified = folderStats[folderPath].lastModified;
          if (
            lastModified &&
            (!currentLastModified || lastModified > currentLastModified)
          ) {
            folderStats[folderPath].lastModified = lastModified;
          }

          if (lastModified) {
            const monthKey = getMonthKey(lastModified);
            if (!folderStats[folderPath].monthlyStats[monthKey]) {
              folderStats[folderPath].monthlyStats[monthKey] = {
                fileCount: 0,
                totalSize: 0,
                averageSize: 0,
              };
            }

            const monthStats = folderStats[folderPath].monthlyStats[monthKey];
            monthStats.fileCount++;
            monthStats.totalSize += size;
            monthStats.averageSize =
              monthStats.totalSize / monthStats.fileCount;
          }

          folderStats[folderPath].averageSize =
            folderStats[folderPath].totalSize /
            folderStats[folderPath].fileCount;
        }
      }

      if (response.IsTruncated) {
        continuationToken = (response as ListObjectsV2CommandOutput)
          .NextContinuationToken;
        // eslint-disable-next-line no-console
        console.log("Fetching next batch of files...");
      } else {
        // eslint-disable-next-line no-console
        console.log("Reached end of bucket");
        break;
      }
    } while (true);

    // Second pass: Analyze previews folder
    // eslint-disable-next-line no-console
    console.log("\nStarting previews folder analysis...");
    continuationToken = undefined;

    do {
      const command: ListObjectsV2Command = new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: "previews/",
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
      });

      const response: ListObjectsV2CommandOutput = await s3Client.send(command);

      if (!response.Contents || response.Contents.length === 0) {
        break;
      }

      // eslint-disable-next-line no-console
      console.log(`Processing ${response.Contents.length} preview files...`);

      for (const object of response.Contents) {
        if (!object.Key || object.Key === "previews/") {
          continue;
        }

        // Extract the unique key from the preview path (e.g., "previews/key1/file.mp4" -> "key1")
        const [, previewKey] = object.Key.split("/");
        const parentFolder = keyToFolderMap[previewKey];

        if (parentFolder && folderStats[parentFolder]) {
          const size = object.Size || 0;
          folderStats[parentFolder].previewStats.totalSize += size;
          folderStats[parentFolder].previewStats.fileCount++;

          // Add preview folder to list if not already there
          if (
            !folderStats[parentFolder].previewStats.previewFolders.includes(
              previewKey
            )
          ) {
            folderStats[parentFolder].previewStats.previewFolders.push(
              previewKey
            );
          }
        }
      }

      continuationToken = (response as ListObjectsV2CommandOutput)
        .NextContinuationToken;
    } while (continuationToken);

    // After both passes, analyze database statistics
    // eslint-disable-next-line no-console
    console.log("\nAnalyzing database statistics...");

    for (const [folder, stats] of Object.entries(folderStats)) {
      // Get all videos in this folder
      const stmt = db.prepare(`SELECT 
          COUNT(CASE WHEN seen IS NOT NULL THEN 1 END) as seen_count,
          COUNT(CASE WHEN favorite = 1 THEN 1 END) as favorite_count,
          MAX(seen) as last_seen,
          AVG(
            (LENGTH(prediction) - LENGTH(REPLACE(prediction, '1', ''))) * 1.0
          ) as avg_prediction,
          AVG(
            CASE 
              WHEN favorite = 1 THEN (LENGTH(prediction) - LENGTH(REPLACE(prediction, '1', ''))) * 1.0
              ELSE NULL 
            END
          ) as avg_favorite_prediction
        FROM videos 
        WHERE key LIKE ?`);

      const rawDbStats = stmt.get(`${folder}/%`);
      const dbStats = rawDbStats ? {
        seen_count: rawDbStats.seen_count as number,
        favorite_count: rawDbStats.favorite_count as number,
        last_seen: rawDbStats.last_seen as string | null,
        avg_prediction: rawDbStats.avg_prediction as number,
        avg_favorite_prediction: rawDbStats.avg_favorite_prediction as number
      } : undefined;

      if (dbStats) {
        stats.dbStats.totalSeen = dbStats.seen_count;
        stats.dbStats.totalFavorites = dbStats.favorite_count;
        stats.dbStats.lastSeenDate = dbStats.last_seen
          ? new Date(dbStats.last_seen)
          : null;
        stats.dbStats.averagePrediction =
          Math.round(dbStats.avg_prediction * 10) / 10 || 0;
        stats.dbStats.averageFavoritePrediction =
          Math.round(dbStats.avg_favorite_prediction * 10) / 10 || 0;
      }
    }

    // Modify the output to include prediction statistics
    const output = Object.entries(folderStats)
      .sort(([, a], [, b]) => b.fileCount - a.fileCount)
      .map(([folder, stats]) => {
        const monthlyStatsStr = Object.entries(stats.monthlyStats)
          .sort(([a], [b]) => b.localeCompare(a))
          .map(([month, monthStats]) => {
            return [
              `    ${month}:`,
              `      Files: ${monthStats.fileCount}`,
              `      Total Size: ${formatBytes(monthStats.totalSize)}`,
              `      Average Size: ${formatBytes(monthStats.averageSize)}`,
            ].join("\n");
          })
          .join("\n");

        const previewStatsStr = [
          "  Preview Statistics:",
          `    Total Preview Folders: ${stats.previewStats.previewFolders.length}`,
          `    Total Preview Files: ${stats.previewStats.fileCount}`,
          `    Total Preview Size: ${formatBytes(
            stats.previewStats.totalSize
          )}`,
          `    Average Size per Preview: ${formatBytes(
            stats.previewStats.previewFolders.length
              ? stats.previewStats.totalSize /
              stats.previewStats.previewFolders.length
              : 0
          )}`,
        ].join("\n");

        const dbStatsStr = [
          "  Database Statistics:",
          `    Videos Watched: ${stats.dbStats.totalSeen}/${stats.fileCount
          } (${Math.round(
            (stats.dbStats.totalSeen / stats.fileCount) * 100
          )}%)`,
          `    Favorite Videos: ${stats.dbStats.totalFavorites}/${stats.fileCount
          } (${Math.round(
            (stats.dbStats.totalFavorites / stats.fileCount) * 100
          )}%)`,
          `    Average Prediction Score: ${stats.dbStats.averagePrediction}/100`,
          `    Average Favorite Prediction Score: ${stats.dbStats.averageFavoritePrediction}/100`,
          `    Last Watched: ${formatDate(stats.dbStats.lastSeenDate)}`,
        ].join("\n");

        return [
          `Folder: ${folder}`,
          `  Files: ${stats.fileCount}`,
          `  Total Size: ${formatBytes(stats.totalSize)}`,
          `  Average Size: ${formatBytes(stats.averageSize)}`,
          `  Last Modified: ${formatDate(stats.lastModified)}`,
          dbStatsStr,
          previewStatsStr,
          "  Monthly Statistics:",
          monthlyStatsStr,
          "",
        ].join("\n");
      })
      .join("\n");

    // After regular stats, analyze zero prediction files
    // eslint-disable-next-line no-console
    console.log("\nAnalyzing zero prediction files...");

    interface ZeroPredictionFile {
      key: string;
      prediction: string;
    }

    interface ZeroPredictionStat {
      key: string;
      size: number;
      previewSize: number;
    }

    // Build the query conditions safely
    const doNotDeleteConditions = DO_NOT_DELETE.length > 0
      ? DO_NOT_DELETE.map((folder) => `key NOT LIKE '${folder}/%'`).join(" AND ")
      : "1=1"; // Always true if no folders to exclude

    const toDeleteConditions = TO_DELETE.length > 0
      ? TO_DELETE.map((folder) => `key LIKE '${folder}/%'`).join(" OR ")
      : "1=0"; // Always false if no folders to delete

    const zeroPredictionStmt = db.prepare(`SELECT key, prediction 
       FROM videos 
       WHERE (
         (
           LENGTH(prediction) - LENGTH(REPLACE(prediction, '1', '')) < 10
           AND key NOT LIKE 'previews/%'
           AND favorite = 0
           AND ${doNotDeleteConditions}
         )
         OR (
           ${toDeleteConditions}
           AND favorite = 0
         )
       )`);

    const rawZeroPredictionResults = zeroPredictionStmt.all();
    const zeroPredictionFiles: ZeroPredictionFile[] = rawZeroPredictionResults.map(row => ({
      key: row.key as string,
      prediction: row.prediction as string
    }));

    let totalPotentialSavings = 0;
    let totalPreviewSavings = 0;
    const zeroPredictionStats: ZeroPredictionStat[] = [];

    for (const file of zeroPredictionFiles) {
      const folderPath = path.dirname(file.key);
      const fileKey = extractKeyFromPath(file.key);

      // Skip protected folders (extra safety check)
      if (DO_NOT_DELETE.includes(folderPath)) {
        continue;
      }

      // Find the file size from our folder stats
      const folder = folderStats[folderPath];
      if (folder) {
        // Since we don't store individual file sizes, we'll use the folder's average
        const estimatedSize = folder.averageSize;
        totalPotentialSavings += estimatedSize;

        // Calculate preview size if this file has previews
        let previewSize = 0;
        if (folder.previewStats.previewFolders.includes(fileKey)) {
          previewSize =
            folder.previewStats.totalSize /
            folder.previewStats.previewFolders.length;
          totalPreviewSavings += previewSize;
        }

        zeroPredictionStats.push({
          key: file.key,
          size: estimatedSize,
          previewSize,
        });
      }
    }

    const zeroPredictionOutput = [
      "\nZero Prediction Files Analysis",
      "============================",
      "Including:",
      "  - Files with prediction < 10 (excluding protected folders)",
      "  - All non-favorite files from folders: " + TO_DELETE.join(", "),
      "",
      "Excluding:",
      "  - All files from protected folders: " + DO_NOT_DELETE.join(", "),
      "  - All favorite files",
      "",
      `Total Files to Delete: ${zeroPredictionFiles.length}`,
      `Main Files Size: ${formatBytes(totalPotentialSavings)}`,
      `Preview Files Size: ${formatBytes(totalPreviewSavings)}`,
      `Total Potential Space Savings: ${formatBytes(
        totalPotentialSavings + totalPreviewSavings
      )}`,
      "\nFiles List:",
      ...zeroPredictionStats.map(
        (file) =>
          `  ${file.key} (Main: ${formatBytes(file.size)}, Preview: ${formatBytes(
            file.previewSize
          )})`
      ),
      "",
    ].join("\n");

    // Write both the regular stats and zero prediction analysis
    await fs.writeFile(
      "bucket-stats.txt",
      output + "\n" + zeroPredictionOutput
    );

    // eslint-disable-next-line no-console
    console.log(`\nAnalysis complete! Processed ${totalFiles} total files`);
    // eslint-disable-next-line no-console
    console.log("Results written to bucket-stats.txt");
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error("Error analyzing bucket:", error);
    throw error;
  } finally {
    db.close();
  }
}

// Run the analysis
// eslint-disable-next-line no-console
analyzeBucket().catch(console.error);
