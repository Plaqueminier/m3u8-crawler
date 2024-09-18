#!/bin/bash

# Directory containing the .ts files
DIRECTORY="/root/m3u8-crawler/videos/"

# Interval in seconds (20 minutes)
INTERVAL=600

# Number of threads
NUM_THREADS=$1

# Function to process .ts files
process_files() {
  for FILE in "$DIRECTORY"/*.ts; do
    if [[ -f "$FILE" ]]; then
      # Extract the base name without extension
      BASENAME=$(basename "$FILE" .ts)
      # Output file name
      OUTPUT="$DIRECTORY/$BASENAME.mp4"

      if [[ ! -f "$OUTPUT" ]]; then
        # Run ffmpeg command
        ffmpeg -i "$FILE" -vf scale=1280:720 -c:v libx264 -crf 28 -preset medium -c:a aac -threads $NUM_THREADS "$OUTPUT"
        # Check if ffmpeg command was successful
        if [[ $? -eq 0 ]]; then
            # Delete the .ts file if conversion was successful
            rm "$FILE"
        else
            echo "Error processing $FILE"
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
