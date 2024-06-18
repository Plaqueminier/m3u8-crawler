import { createLogger, format, transports, Logger } from "winston";

// Define the custom format for the logger
const customFormat = format.combine(
  format.timestamp({
    format: "YYYY-MM-DD HH:mm:ss", // Format the timestamp
  }),
  format.printf((info) => {
    const logEntry = {
      timestamp: info.timestamp,
      level: info.level,
      message: info.message,
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
  ],
});

export default logger;
