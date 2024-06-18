#!/bin/bash

# Directory containing the .ts files
DIRECTORY="/root/m3u8-crawler/videos/"

# Interval in seconds (20 minutes)
INTERVAL=1200

# Function to process .ts files
process_files() {
  for FILE in "$DIRECTORY"/*.mp4; do
    if [[ -f "$FILE" ]]; then
      BASENAME=$(basename "$FILE" .mp4)
      TS_FILE="$DIRECTORY/$BASENAME.ts"
      # Run command
      if [[ ! -f "$TS_FILE" ]]; then
        pnpm ts-node src/upload.ts $FILE
        if [[ $? -eq 0 ]]; then
          # Delete the .ts file if conversion was successful
          rm "$FILE"
        else
          echo "Error uploading $FILE"
        fi
      fi
    fi
  done
}

# Infinite loop to run the process every 20 minutes
while true; do
  process_files
  echo "Waiting..."
  sleep $INTERVAL
done
