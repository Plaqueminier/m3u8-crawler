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

  currentUsernames: string[] = ["", ""];
  currentInputDirectories: string[] = ["", ""];
  currentPageFilesNumber: Set<number>[] = [new Set(), new Set()];

  outputFileDirs: (string | undefined)[] = [];

  reset = (): void => {
    this.currentUsernames = ["", ""];
    this.currentInputDirectories = ["", ""];
    this.currentPageFilesNumber = [new Set(), new Set()];
  };

  onRequest = async (
    request: HTTPRequest,
    fileNumbers: Set<number>,
    inputDirectory: string,
    index: number
  ): Promise<void> => {
    if (request.url().endsWith(".ts")) {
      const url = request.url();
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
      logger.info("Starting ffmpeg...", { metadata: { inputDirectory, outputFile } });
      try {
        const realFileName = await runFFmpeg(fileListPath, outputFile);
        logger.info("ts file created successfully", {
          metadata: { outputFile },
        });
        const sourcePath = path.join(realFileName);
        const destinationPath = path.join("videos", realFileName);
        logger.info("Moving file to videos folder...", {
          metadata: { sourcePath, destinationPath },
        });
        fs.renameSync(sourcePath, destinationPath);
        deleteTmpFiles(fileListPath, inputDirectory);
      } catch (error) {
        logger.error("An error occurred while concatenating the files:", {
          metadata: { error },
        });
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
    const username = await findPerson(index);
    if (username !== this.currentUsernames[index]) {
      if (!username) {
        logger.warn("No username found, retrying in 5min...", { index });
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
        index,
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
    }

    try {
      await page.bringToFront();
      await setTimeout(500);
    } catch {
      logger.warn("Error while bringing tab to front", { index });
    }

    logger.info("Current downloads", {
      metadata: {
        username,
        index,
        downloads: this.currentPageFilesNumber[index]?.size ?? 0,
      },
    });
    if (!this.HAD_NEW_REQUEST[index]) {
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
      const pageNb = Number(process.argv[2]) || 2;
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

      this.reset();

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
