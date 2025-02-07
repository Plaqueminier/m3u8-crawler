import path from "path";
import fs from "fs";
import puppeteer, { Browser, HTTPRequest, Page } from "puppeteer";
import { findPerson } from "./scraper";
import { setTimeout } from "timers/promises";
import { formatDate } from "./utils";
import {
  deleteTmpFiles,
  generateFileList,
  removeEmptyFiles,
  runFFmpeg,
} from "./files";
import logger from "./logger";
import { compact, uniq } from "lodash";

class Crawler {
  HAD_NEW_REQUEST = [true, true];
  MAX_ATTEMPTS = 3;
  SHOULD_STOP = false;

  currentUsernames: string[] = [];
  currentInputDirectories: string[] = [];
  currentPageFilesNumber: Set<number>[] = [];

  outputFileDirs: (string | undefined)[] = [];

  offset = 0;

  reset = (pageNb: number): void => {
    this.currentUsernames = new Array(pageNb).fill("");
    this.currentInputDirectories = new Array(pageNb).fill("");
    this.currentPageFilesNumber = new Array(pageNb).fill(new Set());
  };

  onRequest = async (
    request: HTTPRequest,
    fileNumbers: Set<number>,
    inputDirectory: string,
    index: number
  ): Promise<void> => {
    if (request.url().endsWith(".ts")) {
      const url = request.url();
      try {
        const response = await fetch(request.url());
        const buffer = Buffer.from(await response.arrayBuffer());
        if (buffer.byteLength === 0) {
          request.continue();
          return;
        }
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
        this.HAD_NEW_REQUEST[index] = true;
      } catch (error) {
        logger.error("An error occurred while processing the request:", {
          metadata: { error },
        });
      }
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
      logger.info("Starting ffmpeg...", {
        metadata: { inputDirectory, outputFile },
      });
      const outputFileName = `${outputFile}-${formatDate(new Date())}.ts`;
      const sourcePath = path.join(outputFileName);
      const destinationPath = path.join("videos", outputFileName);
      try {
        await runFFmpeg(fileListPath, outputFileName);
        logger.info("ts file created successfully", {
          metadata: { outputFileName },
        });
        logger.info("Moving file to videos folder...", {
          metadata: { sourcePath, destinationPath },
        });
        fs.renameSync(sourcePath, destinationPath);
        deleteTmpFiles(fileListPath, inputDirectory);
      } catch (error) {
        logger.error("An error occurred while concatenating the files:", {
          metadata: { error },
        });

        if (fs.existsSync(sourcePath)) {
          logger.info(
            "Destination file already exists, considering as success and moving it to videos folder",
            {
              metadata: { sourcePath, destinationPath },
            }
          );
          fs.renameSync(sourcePath, destinationPath);
          deleteTmpFiles(fileListPath, inputDirectory);
          return;
        }

        if (attempt < this.MAX_ATTEMPTS) {
          logger.info("Retrying...");
          return this.concatVideos(
            fileListPath,
            outputFile,
            inputDirectory,
            attempt + 1
          );
        }
        logger.error("Max attempts reached. Exiting...", {});
      }
    } catch (error) {
      logger.error("An error occurred:", {
        metadata: { error },
      });
    }
  };

  interactions = (): void => {
    process.on("message", (msg) => {
      if (msg === "shutdown") {
        this.SHOULD_STOP = true;
        logger.info("Exiting after 10 minutes maximum...");
      }
    });
  };

  handleTab = async (
    page: Page,
    index: number
  ): Promise<string | undefined> => {
    let firstTime = false;
    const username = await findPerson(index + this.offset);
    if (username !== this.currentUsernames[index]) {
      if (!username) {
        logger.warn("No username found, retrying in 5min...", {
          index: index + this.offset,
        });
        return this.currentInputDirectories[index];
      }
      let inputDirectory = `${username}-${formatDate(new Date())}`;
      if (this.outputFileDirs.some((dir) => dir?.includes(username))) {
        logger.info(
          "Username already processed, not creating new directory...",
          {
            metadata: { username },
          }
        );
        inputDirectory =
          this.outputFileDirs.find((dir) => dir?.includes(username)) ||
          inputDirectory;
      }
      this.currentInputDirectories[index] = inputDirectory;
      this.currentUsernames[index] = username;
      this.currentPageFilesNumber[index] = new Set();
      const url = `${process.env.URL}/${username}/`;

      logger.info("Using directory", {
        index: index + this.offset,
        metadata: { inputDirectory },
      });

      page.removeAllListeners("request");

      page.on("request", (request) => {
        this.onRequest(
          request,
          this.currentPageFilesNumber[index],
          this.currentInputDirectories[index],
          index
        );
      });

      await page.goto(url);
      firstTime = true;
    }

    try {
      await page.bringToFront();
      await setTimeout(5000);
    } catch {
      logger.warn("Error while bringing tab to front", {
        index: index + this.offset,
      });
    }

    logger.info("Current downloads", {
      metadata: {
        username,
        index: index + this.offset,
        downloads: this.currentPageFilesNumber[index]?.size ?? 0,
      },
    });
    if (!this.HAD_NEW_REQUEST[index] && !firstTime) {
      logger.info("No new requests, reloading page...", {
        index: index + this.offset,
      });
      page.reload();
      return this.currentInputDirectories[index];
    }
    this.HAD_NEW_REQUEST[index] = false;
    return this.currentInputDirectories[index];
  };

  launch = async (): Promise<void> => {
    while (true) {
      const browser: Browser = await puppeteer.launch({
        defaultViewport: { width: 1920, height: 1080 },
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-session-crashed-bubble",
          "--disable-accelerated-2d-canvas",
          "--no-first-run",
          "--no-zygote",
          "--single-process",
          "--noerrdialogs",
          "--disable-gpu",
        ],
      });
      this.offset = Number(process.argv[3]) || 0;
      const pageNb = Number(process.argv[2]) || 2;
      this.reset(pageNb);
      for (let i = 1; i < pageNb; i++) {
        await browser.newPage();
      }

      const pages = await browser.pages();
      this.interactions();

      for (const page of pages) {
        await page.setRequestInterception(true);
      }

      let minutesElapsed = 0;
      this.outputFileDirs = [];
      while (!this.SHOULD_STOP && minutesElapsed < 120) {
        const tabResults = await Promise.all(
          pages.map((page, index) => this.handleTab(page, index))
        );
        logger.info("Tab results", { metadata: { tabResults } });
        this.outputFileDirs.push(...compact(tabResults));
        this.outputFileDirs = uniq(this.outputFileDirs);
        logger.info("Waiting for new requests", {
          metadata: { dirs: this.outputFileDirs },
        });
        await setTimeout(60_000 * 10);
        minutesElapsed += 10;
      }

      await browser.close();

      this.reset(pageNb);

      logger.info("Start creating videos...", {
        metadata: { outputFileDirs: this.outputFileDirs },
      });
      for (const outputDirectory of this.outputFileDirs) {
        if (!outputDirectory) {
          continue;
        }
        const inputDirectory = path.join(process.cwd(), outputDirectory);
        if (!fs.existsSync(inputDirectory)) {
          continue;
        }
        const outputFileName = path.basename(outputDirectory);
        await removeEmptyFiles(inputDirectory);
        const fileListPath = generateFileList(inputDirectory);
        await this.concatVideos(
          fileListPath,
          outputFileName,
          inputDirectory,
          0
        );
      }

      if (this.SHOULD_STOP) {
        break;
      }
    }

    process.exit();
  };
}

const crawler = new Crawler();
crawler.launch();
