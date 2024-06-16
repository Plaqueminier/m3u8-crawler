import path from "path";
import fs from "fs";
import puppeteer, { Browser, HTTPRequest, Page } from "puppeteer";
const { exec } = require("child_process");
const { setTimeout } = require("timers/promises");

// Usage example
const outputFileName = process.argv[2];
const inputDirectory = process.argv[3];
const maxAttempts = 3;
const timeout = 60_000 * (Number(process.argv[4]) ?? 30);

function formatDate(date: Date): string {
  const pad = (n: number) => ("0" + n).slice(-2);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate()
  )}_${pad(date.getHours())}-${pad(date.getMinutes())}-${pad(
    date.getSeconds()
  )}`;
}

const runFFmpeg = async (
  fileListPath: string,
  outputFile: string
): Promise<void> => {
  const concatCommand = `ffmpeg -loglevel error -f concat -safe 0 -i "${fileListPath}" -c copy "${outputFile}-${formatDate(
    new Date()
  )}.ts"`;
  await execPromise(concatCommand);
};

const deleteTmpFiles = async (
  fileListPath: string,
  inputDir: string
): Promise<void> => {
  // Delete the filelist.txt file
  fs.unlinkSync(fileListPath);
  fs.rmSync(inputDir, { recursive: true, force: true });
  console.log("filelist.txt has been deleted.");
};

const generateFileList = (inputDir: string): string => {
  const files = fs
    .readdirSync(inputDir)
    .filter((file) => path.extname(file) === ".ts");
  // Create the file list
  const fileListPath = path.join(inputDir, "filelist.txt");
  const fileListContent = files.map((file) => `file '${file}'`).join("\n");
  fs.writeFileSync(fileListPath, fileListContent);
  return fileListPath;
};

const concatVideos = async (
  fileListPath: string,
  outputFile: string,
  attempt: number
): Promise<void> => {
  try {
    console.log("Starting ffmpeg...");
    // Concatenate the files using ffmpeg
    try {
      await runFFmpeg(fileListPath, outputFile);
      console.log("ts file created successfully, now mp4...");
    } catch (error) {
      console.error("An error occurred while concatenating the files:", error);
      if (attempt < maxAttempts) {
        console.log("Retrying...");
        return concatVideos(fileListPath, outputFile, attempt + 1);
      } else {
        console.error("Max attempts reached. Exiting...");
      }
    }

    // Delete the filelist.txt file
    deleteTmpFiles(fileListPath, inputDirectory);
  } catch (error) {
    console.error("An error occurred:", error);
  }
};

const execPromise = (command: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    const process = exec(command);

    process.stdout?.on("data", (data: any) => {
      console.log(data.toString());
    });

    process.stderr?.on("data", (data: any) => {
      console.error(data.toString());
    });

    process.on("close", (code: any) => {
      if (code === 0) {
        resolve("Command executed successfully");
      } else {
        reject(new Error(`Command failed with code ${code}`));
      }
    });
  });
};

const onRequest = async (request: HTTPRequest, fileNumbers: Set<number>) => {
  if (request.url().endsWith(".ts")) {
    const url = request.url();
    const response = await fetch(request.url());
    const buffer = Buffer.from(await response.arrayBuffer());
    const urlPath =
      new URL(request.url()).pathname.split("/").pop() || "index.html";
    const filePath = path.join(inputDirectory, urlPath);

    const fileNumber = Number(
      url.slice(url.lastIndexOf("_") + 1, url.lastIndexOf("."))
    );

    if (fileNumbers.has(fileNumber)) {
      request.continue();
      return;
    }

    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, buffer);

    fileNumbers.add(fileNumber);

    console.log(
      `Downloaded and saved to ${filePath}, ${fileNumbers.size} files downloaded`
    );
  }
  request.continue();
};

const main = async () => {
  // Launch the browser
  const browser: Browser = await puppeteer.launch({
    defaultViewport: { width: 1920, height: 1080 },
  });
  // Open a new page
  const page: Page = await browser.newPage();

  // Enable request interception
  await page.setRequestInterception(true);

  const fileNumbers: Set<number> = new Set();

  // Event listener for intercepted requests
  page.on("request", async (request) => {
    onRequest(request, fileNumbers);
  });

  const url = `${outputFileName}/`;
  // Go to the desired webpage
  await page.goto(url);
  console.log(`Page ${url} loaded successfully`);

  await setTimeout(timeout);

  await browser.close();
  console.log("Browser closed");

  const fileListPath = generateFileList(inputDirectory);
  await concatVideos(fileListPath, outputFileName, 0);
};

main();
