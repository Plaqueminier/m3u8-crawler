#!/bin/bash

# Directory containing the .ts files
DIRECTORY="/root/m3u8-crawler/videos/"

# Interval in seconds (20 minutes)
INTERVAL=600

# Function to process .ts files
process_files() {
  for FILE in "$DIRECTORY"/*.mp4; do
    if [[ -f "$FILE" ]]; then
      BASENAME=$(basename "$FILE" .mp4)
      TS_FILE="$DIRECTORY/$BASENAME.ts"

      file_size=$(stat --format="%s" "$FILE")

      local max_size=$((60 * 1024 * 1024)) # 60MB in bytes

      if [ "$file_size" -lt "$max_size" ]; then
        rm "$DIRECTORY/$BASENAME.mp4"
        echo "Deleted $DIRECTORY/$BASENAME.mp4 (size: $file_size bytes)"
      else
        echo "Skipped $DIRECTORY/$BASENAME.mp4 (size: $file_size bytes)"
      fi
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
