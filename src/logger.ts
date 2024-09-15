import { createLogger, format, transports, Logger } from "winston";
import dotenv from "dotenv";
dotenv.config();
// eslint-disable-next-line @typescript-eslint/no-var-requires
const LokiTransport = require("winston-loki");

// Define the custom format for the logger
const customFormat = format.combine(
  format.timestamp({
    format: "YYYY-MM-DD HH:mm:ss", // Format the timestamp
  }),
  format.printf((info) => {
    const logEntry = {
      app: "m3u8-crawler",
      timestamp: info.timestamp,
      level: info.level,
      message: info.message,
      index: info.index,
      ...(info.metadata && { metadata: info.metadata }), // Include meta if present
    };
    return JSON.stringify(logEntry, null, 2); // Pretty-print with 2 spaces of indentation
  })
);

// Instantiate a Winston logger with the custom format and console transport
const logger: Logger = createLogger({
  level: "info", // Set the default log level
  format: customFormat,
  transports: [
    new transports.Console(), // Log to the console
    new LokiTransport({
      host: process.env.LOKI_ENDPOINT,
      json: true,
      format: customFormat,
      labels: { app: "m3u8-crawler", level: "info" },
    }),
  ],
});

export default logger;
