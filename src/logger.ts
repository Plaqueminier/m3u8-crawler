import { createLogger, format, transports, Logger, LeveledLogMethod } from "winston";

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
      ...(info.meta && { meta: info.meta }), // Include meta if present
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

// Extend logger to handle an additional meta object
logger.info = ((message: string, meta?: object) => {
  if (meta) {
    logger.log("info", message, { ...meta });
  } else {
    logger.log("info", message);
  }
}) as LeveledLogMethod;

logger.error = ((message: string, meta?: object) => {
  if (meta) {
    logger.log("error", message, { ...meta });
  } else {
    logger.log("error", message);
  }
}) as LeveledLogMethod;

export default logger;
