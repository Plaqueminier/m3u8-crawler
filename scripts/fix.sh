#!/bin/bash

touch 2024.ts
rm *2024*.ts
find . -maxdepth 1 -type d -not -name "temp_frames" -not -name "venv" -not -name "*$(date '+%Y-%m-%d')*" -not -name "." -not -name "node_modules" -not -name "src" -not -name "scripts" -not -name "videos" -not -name ".git" | while read folder; do
    # Extract the base name of the folder (removing the path)
    base_folder=$(basename "$folder")
    
    echo "Processing folder: $base_folder"
    
    # Run your command sequence, replacing 'key' with the folder name
    pnpm ts-node src/fix.ts "$base_folder" "$base_folder" && \
    rm -rf "$base_folder"/ && \
    mv "$base_folder"*.ts videos
done