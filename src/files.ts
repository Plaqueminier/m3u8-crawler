import { execPromise, formatDate } from "./utils";
import path from "path";
import fs from "fs";

export const runFFmpeg = async (
  fileListPath: string,
  outputFile: string
): Promise<void> => {
  const concatCommand = `ffmpeg -loglevel error -f concat -safe 0 -i "${fileListPath}" -c copy "${outputFile}-${formatDate(
    new Date()
  )}.ts"`;
  await execPromise(concatCommand);
};


export const deleteTmpFiles = async (
  fileListPath: string,
  inputDir: string
): Promise<void> => {
  // Delete the filelist.txt file
  fs.unlinkSync(fileListPath);
  fs.rmSync(inputDir, { recursive: true, force: true });
  console.log("filelist.txt has been deleted.");
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