import path, { format } from "path";
import fs from "fs";
import puppeteer, { Browser, HTTPRequest, Page } from "puppeteer";
import { scrapAndFindPerson } from "./scraper";
import yargs from "yargs";
import { setTimeout } from "timers/promises";
import EventEmitter from "events";
import { execPromise, formatDate } from "./utils";
import { deleteTmpFiles, generateFileList, runFFmpeg } from "./files";
const { exec } = require("child_process");

class Crawler {
  HAD_NEW_REQUEST = true;
  MAX_ATTEMPTS = 3;
  SHOULD_STOP = false;

  onRequest = async (
    request: HTTPRequest,
    fileNumbers: Set<number>,
    inputDirectory: string
  ) => {
    if (request.url().endsWith(".ts")) {
      const url = request.url();
      const response = await fetch(request.url());
      const buffer = Buffer.from(await response.arrayBuffer());
      const urlPath =
        `${formatDate(new Date())}${new URL(request.url()).pathname
          .split("/")
          .pop()}` || "error";
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
      this.HAD_NEW_REQUEST = true;
    }
    request.continue();
  };

  concatVideos = async (
    fileListPath: string,
    outputFile: string,
    inputDirectory: string,
    attempt: number
  ): Promise<void> => {
    try {
      console.log("Starting ffmpeg...");
      // Concatenate the files using ffmpeg
      try {
        await runFFmpeg(fileListPath, outputFile);
        console.log("ts file created successfully, now mp4...");
      } catch (error) {
        console.error(
          "An error occurred while concatenating the files:",
          error
        );
        if (attempt < this.MAX_ATTEMPTS) {
          console.log("Retrying...");
          return this.concatVideos(
            fileListPath,
            outputFile,
            inputDirectory,
            attempt + 1
          );
        } else {
          console.error("Max attempts reached. Exiting...");
        }
      }
    } catch (error) {
      console.error("An error occurred:", error);
    }
  };

  interactions = () => {
    // Create an instance of EventEmitter
    const eventEmitter = new EventEmitter();

    // Define a custom event listener
    eventEmitter.on("keyPress", (key) => {
      if (key === "q") {
        this.SHOULD_STOP = true;
      }
    });

    function handleKeyPress(key: any) {
      if (key === "\u0003") {
        // Ctrl+C
        console.log("\nExiting...");
        process.exit();
      } else {
        eventEmitter.emit("keyPress", key);
      }
    }

    // Set up stdin to listen for key presses
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding("utf8");

    process.stdin.on("data", handleKeyPress);
  };

  launch = async () => {
    const argv = await yargs
      .option("minutes", {
        alias: "m",
        description: "Will close after the specified minutes",
        type: "number",
      })
      .option("name", {
        alias: "n",
        description: "Provide the username",
        type: "string",
      })
      .option("index", {
        alias: "i",
        description: "Provide the index",
        type: "number",
        default: 0,
      })
      .help()
      .alias("help", "h").argv;

    // Launch the browser
    const browser: Browser = await puppeteer.launch({
      defaultViewport: { width: 1920, height: 1080 },
    });
    // Open a new page
    const page: Page = await browser.newPage();

    const outputFileName = argv.name
      ? argv.name
      : await scrapAndFindPerson(page, argv.index);

    if (!outputFileName) {
      console.error("No username found");
      await browser.close();
      return;
    }
    const inputDirectory = `${outputFileName}-${formatDate(new Date())}`;

    await page.setRequestInterception(true);

    const fileNumbers: Set<number> = new Set();

    // Event listener for intercepted requests
    page.on("request", async (request) => {
      this.onRequest(request, fileNumbers, inputDirectory);
    });

    this.interactions();

    const url = `${outputFileName}/`;
    // Go to the desired webpage
    await page.goto(url);
    console.log(`Page ${url} loaded successfully`);

    if (argv.minutes) {
      await setTimeout(60_000 * Number(argv.minutes));
    } else {
      while (this.HAD_NEW_REQUEST && !this.SHOULD_STOP) {
        console.log("Waiting for new requests...");
        this.HAD_NEW_REQUEST = false;
        await setTimeout(60_000 * 5);
      }
      console.log("No new requests, closing...");
    }

    await browser.close();
    console.log("Browser closed");

    const fileListPath = generateFileList(inputDirectory);
    await this.concatVideos(fileListPath, outputFileName, inputDirectory, 0);
    // Delete the filelist.txt file
    deleteTmpFiles(fileListPath, inputDirectory);
  };
}

const crawler = new Crawler();
crawler.launch();
