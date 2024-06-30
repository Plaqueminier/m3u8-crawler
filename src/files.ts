import { execPromise, formatDate } from "./utils";
import path from "path";
import fs from "fs";
import logger from "./logger";

export const runFFmpeg = async (
  fileListPath: string,
  outputFile: string
): Promise<string> => {
  const fileName = `${outputFile}-${formatDate(new Date())}.ts`;
  const concatCommand = `ffmpeg -loglevel error -f concat -safe 0 -i "${fileListPath}" -c copy -threads 1 "${fileName}"`;
  await execPromise(concatCommand);
  return fileName;
};

export const deleteTmpFiles = (
  fileListPath: string,
  inputDir: string
): void => {
  // Delete the filelist.txt file
  fs.unlinkSync(fileListPath);
  fs.rmSync(inputDir, { recursive: true, force: true });
  logger.info("filelist.txt has been deleted.");
};

export const generateFileList = (inputDir: string): string => {
  const files = fs
    .readdirSync(inputDir)
    .filter((file) => path.extname(file) === ".ts");
  // Create the file list
  const fileListPath = path.join(inputDir, "filelist.txt");
  const fileListContent = files.map((file) => `file '${file}'`).join("\n");
  fs.writeFileSync(fileListPath, fileListContent);
  return fileListPath;
};

export const removeEmptyFiles = async (folderPath: string): Promise<void> => {
  try {
    const files = await fs.promises.readdir(folderPath);

    const removeFilePromises = files.map(async (file) => {
      const filePath = path.join(folderPath, file);
      const stat = await fs.promises.stat(filePath);

      if (stat.isFile() && stat.size === 0) {
        await fs.promises.unlink(filePath);
      }
    });

    await Promise.all(removeFilePromises);
  } catch (error) {
    logger.error("Error removing empty files:", error);
  }
};
