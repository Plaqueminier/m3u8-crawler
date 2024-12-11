#!/bin/bash

# Directory containing the .ts files
DIRECTORY="/root/m3u8-crawler/videos/"

# Function to process .ts files
process_files() {
  for FILE in "$DIRECTORY"/*.mp4; do
    if [[ -f "$FILE" ]]; then
      BASENAME=$(basename "$FILE" .mp4)
      TS_FILE="$DIRECTORY/$BASENAME.ts"

      # Run command
      if [[ ! -f "$TS_FILE" ]]; then
        file_size=$(stat --format="%s" "$FILE")

        local max_size=$((60 * 1024 * 1024)) # 60MB in bytes

        if [ "$file_size" -lt "$max_size" ]; then
          rm "$DIRECTORY/$BASENAME.mp4"
          echo "Deleted $DIRECTORY/$BASENAME.mp4 (size: $file_size bytes)"
        else
          pnpm ts-node src/upload.ts $FILE
          if [[ $? -eq 0 ]]; then
            # Delete the .ts file if conversion was successful
            rm "$FILE"
          else
            echo "Error uploading $FILE"
          fi
        fi
        
      fi
    fi
  done
}

process_files
