import logger from "./logger";
import fs from "fs";
import {
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import dotenv from "dotenv";
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

async function uploadFile(filePath: string): Promise<void> {
  const fileStream = fs.createReadStream(filePath);

  const uploadParams = {
    Bucket: BUCKET_NAME,
    Key: filePath.split("/").pop(), // Use the file name as the key
    Body: fileStream,
    ContentType: "video/mp4", // Default content type
  };

  try {
    const command = new PutObjectCommand(uploadParams);
    const response = await s3Client.send(command);
    logger.info("File uploaded successfully:", response);
  } catch (err) {
    logger.error("Error uploading file:", err);
  }
}

const filePath = process.argv[2] ?? "";
uploadFile(filePath);
