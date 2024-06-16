import { exec } from "child_process";

export function formatDate(date: Date): string {
  const pad = (n: number) => ("0" + n).slice(-2);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate()
  )}_${pad(date.getHours())}-${pad(date.getMinutes())}-${pad(
    date.getSeconds()
  )}`;
}

export const execPromise = (command: string): Promise<string> => {
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
