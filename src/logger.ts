import { createLogger, format, transports } from "winston";

// Define the custom format for the logger
const customFormat = format.combine(
  format.timestamp({
    format: "YYYY-MM-DD HH:mm:ss", // Format the timestamp
  }),
  format.printf((info) => {
    return JSON.stringify(
      {
        timestamp: info.timestamp,
        level: info.level,
        message: info.message,
      },
      null,
      2
    );
  }), // Format the output
);

// Instantiate a Winston logger with the custom format and console transport
const logger = createLogger({
  level: "info", // Set the default log level
  format: customFormat,
  transports: [
    new transports.Console(), // Log to the console
  ],
});

export default logger;
