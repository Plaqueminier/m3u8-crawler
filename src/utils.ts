import { exec } from "child_process";
import logger from "./logger";

export function formatDate(date: Date): string {
  const pad = (n: number): string => ("0" + n).slice(-2);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate()
  )}_${pad(date.getHours())}-${pad(date.getMinutes())}-${pad(
    date.getSeconds()
  )}`;
}

export const execPromise = (command: string): Promise<boolean> => {
  return new Promise((resolve, reject) => {
    const process = exec(command);

    process.stdout?.on("data", (data: Buffer) => {
      logger.info(data.toString());
    });

    process.stderr?.on("data", (data: Buffer) => {
      logger.error(data.toString());
      reject(new Error("Command failed"));
    });

    process.on("close", (code: number) => {
      if (code === 0) {
        resolve(true);
      } else {
        reject(new Error(`Command failed with code ${code}`));
      }
    });
  });
};
