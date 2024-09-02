import { exec } from "child_process";
import fs from "fs";
import path from "path";

function formatDate(date: Date): string {
  const pad = (n: number): string => ("0" + n).slice(-2);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate()
  )}_${pad(date.getHours())}-${pad(date.getMinutes())}-${pad(
    date.getSeconds()
  )}`;
}

const execPromise = (command: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    const process = exec(command);

    process.stdout?.on("data", (data: Buffer) => {
      // eslint-disable-next-line no-console
      console.log(data.toString());
    });

    process.stderr?.on("data", (data: Buffer) => {
      // eslint-disable-next-line no-console
      console.error(data.toString());
    });

    process.on("close", (code: number) => {
      if (code === 0) {
        resolve("Command executed successfully");
      } else {
        reject(new Error(`Command failed with code ${code}`));
      }
    });
  });
};

const runFFmpeg = async (
  fileListPath: string,
  outputFile: string
): Promise<void> => {
  const concatCommand = `ffmpeg -loglevel error -f concat -safe 0 -i "${fileListPath}" -c copy "${outputFile}-${formatDate(
    new Date()
  )}.ts"`;
  await execPromise(concatCommand);
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

generateFileList(process.argv[2]);
runFFmpeg(`${process.argv[2]}/filelist.txt`, process.argv[3]);
