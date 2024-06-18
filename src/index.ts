import path from "path";
import fs from "fs";
import puppeteer, { Browser, HTTPRequest } from "puppeteer";
import { scrapAndFindPerson } from "./scraper";
import yargs from "yargs";
import { setTimeout } from "timers/promises";
import EventEmitter from "events";
import { formatDate } from "./utils";
import { deleteTmpFiles, generateFileList, runFFmpeg } from "./files";
import logger from "./logger";

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

      logger.info(
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
      logger.info("Starting ffmpeg...");
      // Concatenate the files using ffmpeg
      try {
        const realFileName = await runFFmpeg(fileListPath, outputFile);
        logger.info("ts file created successfully");
        const destinationPath = path.join(__dirname, "videos", realFileName);
        fs.rename(realFileName, destinationPath, (err) => {
          if (err) {
            logger.error("Error moving file:", err);
          } else {
            logger.info("File moved successfully");
          }
        });
      } catch (error) {
        logger.error("An error occurred while concatenating the files:", error);
        if (attempt < this.MAX_ATTEMPTS) {
          logger.info("Retrying...");
          return this.concatVideos(
            fileListPath,
            outputFile,
            inputDirectory,
            attempt + 1
          );
        }
        logger.error("Max attempts reached. Exiting...");
      }
    } catch (error) {
      logger.error("An error occurred:", error);
    }
  };

  interactions = () => {
    // Create an instance of EventEmitter
    const eventEmitter = new EventEmitter();

    // Define a custom event listener
    eventEmitter.on("keyPress", (key) => {
      if (key === "q") {
        this.SHOULD_STOP = true;
        logger.info("Exiting after 5 minutes maximum...");
      }
    });

    function handleKeyPress(key: string) {
      if (key === "\u0003") {
        // Ctrl+C
        logger.info("\nExiting...");
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

    while (true) {
      // Launch the browser
      const browser: Browser = await puppeteer.launch({
        defaultViewport: { width: 1920, height: 1080 },
      });
      // Open a new page
      const [page] = await browser.pages();

      let outputFileName = argv.name
        ? argv.name
        : await scrapAndFindPerson(page, argv.index);

      while (!outputFileName) {
        logger.info("No username found, retrying...");
        await setTimeout(120_000);
        outputFileName = await scrapAndFindPerson(page, argv.index);
      }

      const inputDirectory = `${outputFileName}-${formatDate(new Date())}`;

      await page.setRequestInterception(true);

      const fileNumbers: Set<number> = new Set();

      // Event listener for intercepted requests
      page.on("request", (request) => {
        this.onRequest(request, fileNumbers, inputDirectory);
      });

      this.interactions();

      const url = `${outputFileName}/`;

      if (argv.minutes) {
        // Go to the desired webpage
        await page.goto(url);

        logger.info(`Page ${url} loaded successfully`);
        await setTimeout(60_000 * Number(argv.minutes));
      } else {
        while (this.HAD_NEW_REQUEST && !this.SHOULD_STOP) {
          const newName = await scrapAndFindPerson(page, argv.index);
          if (newName && newName !== outputFileName) {
            break;
          }
          await page.goto(url);

          logger.info(`Page ${url} loaded successfully`);

          logger.info("Waiting for new requests...");
          this.HAD_NEW_REQUEST = false;
          await setTimeout(60_000 * 5);
        }

        logger.info("No new requests or name in order changed, closing...");
      }

      await browser.close();

      logger.info("Browser closed");

      if (fileNumbers.size === 0) {
        logger.info("No files downloaded, retrying...");
        continue;
      }
      const fileListPath = generateFileList(inputDirectory);
      await this.concatVideos(fileListPath, outputFileName, inputDirectory, 0);
      // Delete the filelist.txt file
      deleteTmpFiles(fileListPath, inputDirectory);

      if (this.SHOULD_STOP) {
        break;
      }
    }
    process.exit();
  };
}

const crawler = new Crawler();
crawler.launch();
