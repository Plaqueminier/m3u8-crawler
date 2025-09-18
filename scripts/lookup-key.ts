import {
    S3Client,
    HeadObjectCommand,
    ListObjectsV2Command,
    DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import * as dotenv from "dotenv";
import { DatabaseSync } from "node:sqlite";

dotenv.config();

const s3Client = new S3Client({
    region: "auto",
    endpoint: process.env.R2_ENDPOINT,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY || "",
        secretAccessKey: process.env.R2_SECRET_KEY || "",
    },
});

interface VideoRecord {
    id: number;
    key: string;
    size: number;
    favorite: number;
    seen: string | null;
    prediction: string;
    lastModified: string;
}

interface S3FileStatus {
    exists: boolean;
    size?: number;
    lastModified?: Date;
    error?: string;
}

interface PreviewStatus {
    folderExists: boolean;
    fileCount: number;
    totalSize: number;
    files: string[];
}

async function checkS3FileExists(bucketName: string, key: string): Promise<S3FileStatus> {
    try {
        const command = new HeadObjectCommand({
            Bucket: bucketName,
            Key: key,
        });

        const response = await s3Client.send(command);

        return {
            exists: true,
            size: response.ContentLength,
            lastModified: response.LastModified,
        };
    } catch (error: any) {
        if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
            return {
                exists: false,
            };
        }

        return {
            exists: false,
            error: error.message,
        };
    }
}

async function checkPreviewFolder(bucketName: string, previewKey: string): Promise<PreviewStatus> {
    try {
        const command = new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: `previews/${previewKey}/`,
            MaxKeys: 1000,
        });

        const response = await s3Client.send(command);

        if (!response.Contents || response.Contents.length === 0) {
            return {
                folderExists: false,
                fileCount: 0,
                totalSize: 0,
                files: [],
            };
        }

        const files = response.Contents
            .filter(obj => obj.Key && obj.Key !== `previews/${previewKey}/`)
            .map(obj => obj.Key!);

        const totalSize = response.Contents
            .filter(obj => obj.Key && obj.Key !== `previews/${previewKey}/`)
            .reduce((sum, obj) => sum + (obj.Size || 0), 0);

        return {
            folderExists: true,
            fileCount: files.length,
            totalSize,
            files,
        };
    } catch (error: any) {
        return {
            folderExists: false,
            fileCount: 0,
            totalSize: 0,
            files: [],
        };
    }
}

async function deletePreviewFolder(bucketName: string, previewKey: string): Promise<boolean> {
    try {
        console.log(`🗑️  Deleting orphaned preview folder: previews/${previewKey}/`);

        // First, get all files in the preview folder
        const command = new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: `previews/${previewKey}/`,
            MaxKeys: 1000,
        });

        const response = await s3Client.send(command);

        if (!response.Contents || response.Contents.length === 0) {
            console.log("   No files found to delete");
            return true;
        }

        // Delete each file
        let deletedCount = 0;
        for (const object of response.Contents) {
            if (object.Key && object.Key !== `previews/${previewKey}/`) {
                try {
                    await s3Client.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: object.Key,
                    }));
                    console.log(`   ✅ Deleted: ${object.Key}`);
                    deletedCount++;
                } catch (error) {
                    console.error(`   ❌ Failed to delete ${object.Key}:`, error);
                }
            }
        }

        console.log(`   🎉 Successfully deleted ${deletedCount} preview files`);
        return true;
    } catch (error) {
        console.error(`   ❌ Error deleting preview folder:`, error);
        return false;
    }
}

function promptUser(question: string): Promise<string> {
    return new Promise((resolve) => {
        process.stdout.write(question);
        process.stdin.once('data', (data) => {
            resolve(data.toString().trim());
        });
    });
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

function formatDate(date: Date | null | undefined): string {
    if (!date) {
        return "N/A";
    }
    return date.toLocaleString();
}

async function lookupKey(searchKey: string): Promise<void> {
    const bucketName = process.env.R2_BUCKET || "";

    if (!searchKey) {
        console.error("Please provide a key to search for");
        console.log("Usage: npm run lookup-key <key>");
        process.exit(1);
    }

    // Setup stdin for interactive prompts
    process.stdin.setEncoding('utf8');

    console.log(`🔍 Looking up key: "${searchKey}"`);
    console.log("=".repeat(50));

    // Open database connection
    const db = new DatabaseSync("../videos.db");

    // Declare relatedDbRecord at function scope to avoid declaration order issues
    let relatedDbRecord: VideoRecord | null = null;

    try {
        // Search for the key in the database - try exact match first, then partial match
        let stmt = db.prepare("SELECT id, key, size, favorite, seen, prediction, lastModified FROM videos WHERE key = ?");
        let rawResult = stmt.get(searchKey);

        // If no exact match, try searching for keys that end with the search key
        if (!rawResult) {
            console.log(`🔍 No exact match found, searching for keys ending with: "${searchKey}"`);
            stmt = db.prepare("SELECT id, key, size, favorite, seen, prediction, lastModified FROM videos WHERE key LIKE ?");
            rawResult = stmt.get(`%/${searchKey}`);

            if (rawResult) {
                console.log(`✅ Found database record with key ending in "${searchKey}": ${rawResult.key}`);
            }
        }

        if (!rawResult) {
            console.log("❌ Key not found in database");
        } else {
            const dbRecord: VideoRecord = {
                id: rawResult.id as number,
                key: rawResult.key as string,
                size: rawResult.size as number,
                favorite: rawResult.favorite as number,
                seen: rawResult.seen as string | null,
                prediction: rawResult.prediction as string,
                lastModified: rawResult.lastModified as string,
            };

            console.log("✅ Database Record Found:");
            console.log(`   ID: ${dbRecord.id}`);
            console.log(`   Key: ${dbRecord.key}`);
            console.log(`   Size: ${formatFileSize(dbRecord.size)}`);
            console.log(`   Favorite: ${dbRecord.favorite ? 'Yes' : 'No'}`);
            console.log(`   Seen: ${dbRecord.seen || 'Never'}`);
            console.log(`   Prediction: ${dbRecord.prediction}`);
            console.log(`   Prediction Score: ${(dbRecord.prediction.match(/1/g) || []).length}/100`);
            console.log(`   Last Modified: ${dbRecord.lastModified}`);
        }

        // Extract preview key - handle both full path and just filename
        let previewKey: string;
        if (searchKey.includes('/')) {
            // If searchKey includes folder, extract just the filename part for previews
            previewKey = searchKey.split('/').pop()?.split('.')[0] || searchKey.split('.')[0];
        } else {
            // If searchKey is just the filename, use it directly for previews
            previewKey = searchKey.split('.')[0];
        }

        console.log(`🔍 Using preview key: "${previewKey}" (extracted from: "${searchKey}")`);

        // If we didn't find the exact key in DB, try to find a related key that matches the preview pattern
        if (!rawResult) {
            console.log("🔍 Searching for related database records...");

            // Try multiple patterns to find the record
            const patterns = [
                `${previewKey}.%`,           // filename.extension
                `%/${previewKey}.%`,         // folder/filename.extension  
                `%/${previewKey}`,           // folder/filename (no extension)
                `${previewKey}`              // just filename (no extension)
            ];

            for (const pattern of patterns) {
                const relatedStmt = db.prepare("SELECT id, key, size, favorite, seen, prediction, lastModified FROM videos WHERE key LIKE ? LIMIT 1");
                const relatedRawResult = relatedStmt.get(pattern);

                if (relatedRawResult) {
                    relatedDbRecord = {
                        id: relatedRawResult.id as number,
                        key: relatedRawResult.key as string,
                        size: relatedRawResult.size as number,
                        favorite: relatedRawResult.favorite as number,
                        seen: relatedRawResult.seen as string | null,
                        prediction: relatedRawResult.prediction as string,
                        lastModified: relatedRawResult.lastModified as string,
                    };
                    console.log(`✅ Found related database record with pattern "${pattern}": ${relatedDbRecord.key}`);
                    break;
                }
            }
        }

        console.log("\n" + "=".repeat(50));

        // Check if main file exists in S3 - use the database key if we found one
        console.log("🗂️  S3 Main File Status:");
        const s3KeyToCheck = rawResult ? (rawResult.key as string) : (relatedDbRecord?.key || searchKey);
        console.log(`🔍 Checking S3 for key: "${s3KeyToCheck}"`);
        const mainFileStatus = await checkS3FileExists(bucketName, s3KeyToCheck);

        if (mainFileStatus.exists) {
            console.log(`   ✅ File exists in S3`);
            console.log(`   Size: ${formatFileSize(mainFileStatus.size || 0)}`);
            console.log(`   Last Modified: ${formatDate(mainFileStatus.lastModified)}`);
        } else {
            console.log(`   ❌ File does not exist in S3`);
            if (mainFileStatus.error) {
                console.log(`   Error: ${mainFileStatus.error}`);
            }
        }

        console.log("\n📁 S3 Preview Folder Status:");
        console.log(`   Preview Key: ${previewKey}`);

        const previewStatus = await checkPreviewFolder(bucketName, previewKey);

        if (previewStatus.folderExists) {
            console.log(`   ✅ Preview folder exists`);
            console.log(`   File Count: ${previewStatus.fileCount}`);
            console.log(`   Total Size: ${formatFileSize(previewStatus.totalSize)}`);
            console.log(`   Files:`);
            previewStatus.files.forEach(file => {
                console.log(`     - ${file}`);
            });
        } else {
            console.log(`   ❌ Preview folder does not exist or is empty`);
        }

        // Summary
        console.log("\n" + "=".repeat(50));
        console.log("📊 Summary:");

        const dbExists = !!rawResult;
        const relatedDbExists = !!relatedDbRecord;
        const s3MainExists = mainFileStatus.exists;
        const s3PreviewExists = previewStatus.folderExists;

        console.log(`   Database (exact): ${dbExists ? '✅' : '❌'}`);
        if (relatedDbExists) {
            console.log(`   Database (related): ✅ (${relatedDbRecord?.key})`);
        }
        console.log(`   S3 Main File: ${s3MainExists ? '✅' : '❌'}`);
        console.log(`   S3 Preview: ${s3PreviewExists ? '✅' : '❌'}`);

        // Identify potential issues
        if ((dbExists || relatedDbExists) && !s3MainExists) {
            // Get the ID and key to delete - handle both exact and related records
            const recordToDelete = dbExists ? {
                id: rawResult?.id as number,
                key: rawResult?.key as string
            } : {
                id: relatedDbRecord?.id as number,
                key: relatedDbRecord?.key as string
            };
            console.log("\n⚠️  WARNING: File exists in database but not in S3 (orphaned DB record)");
            console.log(`   Database record: ${recordToDelete.key} (ID: ${recordToDelete.id})`);

            // Offer to clean up orphaned database record
            console.log("\n🧹 Database Cleanup Options:");
            console.log("   This database record points to a file that no longer exists in S3.");

            const dbAnswer = await promptUser("   Do you want to delete this orphaned database record? (y/N): ");

            if (dbAnswer.toLowerCase() === 'y' || dbAnswer.toLowerCase() === 'yes') {
                try {
                    if (!recordToDelete.id || !recordToDelete.key) {
                        throw new Error("No record found to delete");
                    }

                    // Debug: Show the record info
                    console.log(`   🔍 Debug: Record to delete - ID: ${recordToDelete.id}, Key: "${recordToDelete.key}"`);

                    // First, let's verify the record exists with a SELECT by ID
                    const checkStmt = db.prepare("SELECT COUNT(*) as count FROM videos WHERE id = ?");
                    const checkResult = checkStmt.get(recordToDelete.id) as { count: number };
                    console.log(`   🔍 Debug: Records found with this ID: ${checkResult.count}`);

                    if (checkResult.count === 0) {
                        console.log("   ⚠️  Record not found by ID - this shouldn't happen!");
                        return;
                    }

                    // Delete by ID instead of key to avoid special character issues
                    const deleteDbStmt = db.prepare("DELETE FROM videos WHERE id = ?");
                    const result = deleteDbStmt.run(recordToDelete.id);

                    if (result.changes > 0) {
                        console.log(`   ✅ Deleted orphaned database record: ${recordToDelete.key} (ID: ${recordToDelete.id})`);
                    } else {
                        console.log(`   ⚠️  No record was deleted - ID: ${recordToDelete.id}`);
                    }
                } catch (error: any) {
                    console.error(`   ❌ Failed to delete database record:`, error);

                    if (error.message && error.message.includes('malformed')) {
                        console.log(`   🚨 DATABASE CORRUPTION DETECTED!`);
                        console.log(`   Your SQLite database is corrupted and needs repair.`);
                        console.log(`   Suggested fix:`);
                        console.log(`   1. Backup: cp videos.db videos.db.backup`);
                        console.log(`   2. Repair: sqlite3 videos.db ".dump" | sqlite3 videos_repaired.db`);
                        console.log(`   3. Replace: mv videos_repaired.db videos.db`);
                    }
                }
            } else {
                console.log("   ⏭️  Skipped database cleanup - orphaned record remains");
            }
        }

        if (!dbExists && !relatedDbExists && s3MainExists) {
            console.log("\n⚠️  WARNING: File exists in S3 but not in database (missing DB record)");
        }

        if (s3MainExists && !s3PreviewExists) {
            console.log("\n💡 INFO: Main file exists but no preview folder (may be normal)");
        }

        // Check for orphaned previews - whenever no main file exists in S3
        if (!s3MainExists && s3PreviewExists) {
            console.log("\n⚠️  WARNING: Preview folder exists but main file is missing (ORPHANED PREVIEWS)");
            console.log(`   This wastes ${formatFileSize(previewStatus.totalSize)} of storage space`);

            if (relatedDbExists) {
                console.log(`   Note: Related database record exists: ${relatedDbRecord?.key}`);
                console.log("   But since the main file is missing from S3, these previews are still orphaned.");
            }

            // Offer to clean up orphaned previews
            console.log("\n🧹 Preview Cleanup Options:");
            console.log("   Since no main file exists in S3, these preview files should be deleted.");

            const previewAnswer = await promptUser("   Do you want to delete these orphaned preview files? (y/N): ");

            if (previewAnswer.toLowerCase() === 'y' || previewAnswer.toLowerCase() === 'yes') {
                const success = await deletePreviewFolder(bucketName, previewKey);
                if (success) {
                    console.log(`   ✅ Preview cleanup completed! Freed ${formatFileSize(previewStatus.totalSize)} of storage`);
                } else {
                    console.log("   ❌ Preview cleanup failed - please check the errors above");
                }
            } else {
                console.log("   ⏭️  Skipped preview cleanup - orphaned previews remain");
            }
        }

    } catch (error) {
        console.error("❌ Error during lookup:", error);
        throw error;
    } finally {
        db.close();
        process.stdin.pause();
    }
}

// Get the key from command line arguments
const searchKey = process.argv[2];

if (!searchKey) {
    console.error("Please provide a key to search for");
    console.log("Usage: node scripts/lookup-key.js <key>");
    console.log("Example: node scripts/lookup-key.js folder/video.mp4");
    process.exit(1);
}

// Run the lookup
lookupKey(searchKey).catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
});