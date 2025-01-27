import os
import sys
import sqlite3
import torch
import torch.nn as nn
from torchvision import transforms, models
from PIL import Image
import logging
from typing import List, Tuple, Optional
import shutil
import subprocess
from tqdm import tqdm
import time
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import psutil

# Load environment variables at the start
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("url_predict.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class S3Handler:
    def __init__(self):
        # Ensure environment variables are loaded
        endpoint = os.getenv("R2_ENDPOINT")
        access_key = os.getenv("R2_ACCESS_KEY")
        secret_key = os.getenv("R2_SECRET_KEY")
        self.bucket_name = os.getenv("R2_BUCKET")

        # Validate required environment variables
        if not all([endpoint, access_key, secret_key, self.bucket_name]):
            missing_vars = []
            for var in ["R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET"]:
                if not os.getenv(var):
                    missing_vars.append(var)
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",  # R2 doesn't use regions
        )

    def create_presigned_url(
        self, object_key: str, expiration: int = 3600
    ) -> Optional[str]:
        """Generate a presigned URL for the R2 object"""
        try:
            url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": object_key},
                ExpiresIn=expiration,
            )
            logger.info(f"Generated presigned URL for key: {object_key}")
            return url
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return None


class DatabaseHandler:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_next_unprocessed_video(self) -> Tuple[Optional[int], Optional[str]]:
        """Get the first video that has all zeros prediction or NULL prediction"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT id, key, LENGTH(prediction) - LENGTH(REPLACE(prediction, '1', '')) as quality
                FROM videos
                WHERE predictedAt < '2024-12-18 06:05:00'
                ORDER BY quality DESC, id DESC
                LIMIT 1
            """,
            )

            result = cursor.fetchone()
            conn.close()

            if result:
                return result[0], result[1]
            return None, None

        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return None, None

    def update_prediction(self, row_id: int, prediction: str) -> bool:
        """Update the prediction for a specific row"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE videos SET prediction = ? WHERE id = ?", (prediction, row_id)
            )

            conn.commit()
            conn.close()
            logger.info(f"Successfully updated database for row {row_id}")
            return True

        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return False


class TransferModel(nn.Module):
    def __init__(self, weights=None):
        super(TransferModel, self).__init__()
        self.resnet = models.resnet18(weights=weights)

        # Add batch normalization and dropout layers
        self.resnet.layer1 = nn.Sequential(
            self.resnet.layer1, nn.BatchNorm2d(64), nn.Dropout(0.1)
        )
        self.resnet.layer2 = nn.Sequential(
            self.resnet.layer2, nn.BatchNorm2d(128), nn.Dropout(0.2)
        )
        self.resnet.layer3 = nn.Sequential(
            self.resnet.layer3, nn.BatchNorm2d(256), nn.Dropout(0.2)
        )

        num_ftrs = self.resnet.fc.in_features
        self.resnet.fc = nn.Sequential(
            nn.Linear(num_ftrs, 512),
            nn.BatchNorm1d(512),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(512, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 1),
            nn.Sigmoid(),
        )

    def forward(self, x):
        return self.resnet(x)


class VideoFrameExtractor:
    def __init__(self):
        self.logger = logger

    def check_ffmpeg(self) -> bool:
        """Check if FFmpeg is available"""
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True)
            return True
        except FileNotFoundError:
            self.logger.error("FFmpeg not found. Please install FFmpeg.")
            return False

    def parse_fps(self, fps_str: str) -> float:
        """Parse frame rate string from FFmpeg"""
        if "/" in fps_str:
            num, den = map(int, fps_str.split("/"))
            return num / den
        return float(fps_str)

    def extract_frames(
        self, url: str, output_dir: str = "temp_frames", format: str = "jpg"
    ) -> int:
        """Extract frames using FFmpeg streaming with optimizations"""
        if not self.check_ffmpeg():
            return 0

        self.logger.info(f"Starting frame extraction from URL: {url}")
        self.logger.info(f"Output directory: {output_dir}")

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.logger.info(f"Created output directory: {output_dir}")
        else:
            self.logger.info(f"Cleaning existing output directory: {output_dir}")
            shutil.rmtree(output_dir)
            os.makedirs(output_dir)

        try:
            # Get video duration first
            self.logger.info("Executing FFprobe to get video duration...")
            duration_cmd = [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                url,
            ]

            duration_output = subprocess.run(
                duration_cmd, capture_output=True, text=True
            )
            if duration_output.returncode != 0:
                self.logger.error(f"FFprobe failed: {duration_output.stderr}")
                return 0

            duration = float(duration_output.stdout.strip())
            self.logger.info(f"Video duration: {duration:.2f} seconds")

            # Optimize frame extraction settings
            cmd = [
                "ffmpeg",
                "-y",
                "-loglevel",
                "info",
                "-i",
                url,
                "-vf",
                f"fps=1/{duration / 100}",  # Extract exactly 100 frames
                "-frame_pts",
                "1",
                "-frames:v",
                "100",
                "-threads",
                "2",
                "-preset",
                "ultrafast",
                "-tune",
                "fastdecode",
                "-f",
                "image2",
                "-qscale:v",
                "3",
                os.path.join(output_dir, f"frame_%03d.{format}"),
            ]

            self.logger.info("Starting FFmpeg process with command:")
            self.logger.info(" ".join(cmd))

            # Create process with pipe for both stdout and stderr
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
            )

            def log_output(pipe, prefix):
                """Helper function to log output from pipes"""
                for line in iter(pipe.readline, ""):
                    line = line.strip()
                    if line:
                        if "error" in line.lower():
                            self.logger.error(f"{prefix}: {line}")
                        elif "warning" in line.lower():
                            self.logger.warning(f"{prefix}: {line}")
                        else:
                            self.logger.info(f"{prefix}: {line}")

            # Create threads to handle output logging
            from threading import Thread

            stdout_thread = Thread(target=log_output, args=(process.stdout, "FFmpeg"))
            stderr_thread = Thread(target=log_output, args=(process.stderr, "FFmpeg"))

            # Start threads
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            stdout_thread.start()
            stderr_thread.start()

            last_frame_count = 0
            stall_counter = 0
            last_progress_time = time.time()
            start_time = time.time()

            # Main processing loop
            while process.poll() is None:
                current_frames = len(
                    [
                        f
                        for f in os.listdir(output_dir)
                        if f.lower().endswith(f".{format}")
                    ]
                )

                if current_frames != last_frame_count:
                    frames_delta = current_frames - last_frame_count
                    current_time = time.time()
                    fps = frames_delta / (current_time - last_progress_time)
                    elapsed = current_time - start_time

                    self.logger.info(
                        f"Progress: {current_frames}/100 frames "
                        f"(+{frames_delta} frames, {fps:.1f} fps, "
                        f"elapsed: {elapsed:.1f}s)"
                    )

                    last_frame_count = current_frames
                    last_progress_time = current_time
                    stall_counter = 0
                else:
                    stall_counter += 1
                    if stall_counter == 30:  # Log after 3 seconds of stall
                        self.logger.warning(
                            f"Frame extraction stalled at {current_frames}/100 frames. "
                            "Checking process status..."
                        )

                time.sleep(0.1)

            # Wait for output threads to finish
            stdout_thread.join(timeout=1)
            stderr_thread.join(timeout=1)

            # Get final status
            return_code = process.wait()
            if return_code != 0:
                self.logger.error(f"FFmpeg failed with code {return_code}")
                return 0

            final_frames = len(
                [f for f in os.listdir(output_dir) if f.lower().endswith(f".{format}")]
            )

            total_time = time.time() - start_time
            self.logger.info(
                f"Frame extraction completed. Total frames: {final_frames}, "
                f"Time taken: {total_time:.1f}s, "
                f"Average speed: {final_frames / total_time:.1f} fps"
            )
            return final_frames

        except Exception as e:
            self.logger.error(f"Frame extraction failed: {str(e)}")
            return 0


class URLImageProcessor:
    def __init__(self, model_path: str):
        self.device = torch.device(
            "mps"
            if torch.backends.mps.is_available()
            else "cuda" if torch.cuda.is_available() else "cpu"
        )
        logger.info(f"Using device: {self.device}")

        # Initialize model
        self.model = TransferModel()
        checkpoint = torch.load(model_path, map_location=self.device, weights_only=True)

        if "model_state_dict" in checkpoint:
            self.model.load_state_dict(checkpoint["model_state_dict"])
        else:
            self.model.load_state_dict(checkpoint)

        self.model.to(self.device)
        self.model.eval()

        # Use faster interpolation mode for resizing
        self.transform = transforms.Compose(
            [
                transforms.Resize(
                    (224, 224), interpolation=transforms.InterpolationMode.NEAREST
                ),
                transforms.ToTensor(),
                transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
            ]
        )

        self.frame_extractor = VideoFrameExtractor()
        self.batch_size = 4  # Reduced batch size to lower memory/CPU usage

    def download_frames(self, url: str, output_dir: str = "temp_frames"):
        """Extract frames from video URL"""
        num_frames = self.frame_extractor.extract_frames(url, output_dir)
        if num_frames == 0:
            return []

        # Return paths of extracted frames
        return sorted(
            [
                os.path.join(output_dir, f)
                for f in os.listdir(output_dir)
                if f.lower().endswith((".jpg", ".jpeg", ".png"))
            ]
        )

    @torch.no_grad()
    def predict_batch(self, image_paths: List[str]) -> List[float]:
        """Predict a batch of images at once"""
        try:
            # Process images one at a time to reduce memory usage
            batch_tensors = []
            for img_path in image_paths:
                # Use PIL's draft mode for faster loading
                image = Image.open(img_path)
                image.draft("RGB", (224, 224))
                image = image.convert("RGB")
                image_tensor = self.transform(image)
                batch_tensors.append(image_tensor)
                # Explicitly close image to free memory
                image.close()

            # Clear CUDA cache if using GPU
            if self.device.type == "cuda":
                torch.cuda.empty_cache()

            batch = torch.stack(batch_tensors).to(self.device)
            outputs = self.model(batch)
            probabilities = outputs.squeeze().cpu().numpy().tolist()

            # Clear variables to free memory
            del batch_tensors
            del batch
            del outputs

            if isinstance(probabilities, float):
                probabilities = [probabilities]

            return probabilities

        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            return [None] * len(image_paths)

    def process_url(self, url: str, temp_dir: str = "temp_frames") -> str:
        """Process video URL and return prediction string where each character represents one frame"""
        # Extract frames
        logger.info("Starting frame extraction...")
        frame_paths = self.download_frames(url, temp_dir)
        if not frame_paths:
            return "0" * 100

        predictions = []
        total_batches = (len(frame_paths) + self.batch_size - 1) // self.batch_size
        logger.info(
            f"Starting inference on {len(frame_paths)} frames in {total_batches} batches..."
        )

        # Process frames in batches
        for i in tqdm(
            range(0, len(frame_paths), self.batch_size),
            desc="Processing frames",
            total=total_batches,
        ):
            batch_paths = frame_paths[i : i + self.batch_size]
            logger.info(
                f"Processing batch {(i // self.batch_size) + 1}/{total_batches} ({len(batch_paths)} frames)"
            )
            batch_predictions = self.predict_batch(batch_paths)

            # Filter out None values and convert to binary predictions
            valid_predictions = [
                "1" if p is not None and p > 0.5 else "0" for p in batch_predictions
            ]
            predictions.extend(valid_predictions)

            # Add small sleep between batches to reduce CPU load
            time.sleep(0.1)

        # Clean up
        shutil.rmtree(temp_dir)

        if not predictions:
            return "0" * 100

        logger.info(f"Completed inference. Got {len(predictions)} predictions.")

        # If we have less than 100 predictions, pad with the last prediction
        # If we have more than 100, take the first 100
        if len(predictions) < 100:
            last_pred = predictions[-1]
            predictions.extend([last_pred] * (100 - len(predictions)))
            logger.info(f"Padded predictions to 100 characters with '{last_pred}'")
        elif len(predictions) > 100:
            predictions = predictions[:100]
            logger.info("Truncated predictions to 100 characters")

        return "".join(predictions)


def limit_cpu_usage():
    """Limit the process to use fewer CPU cores"""
    process = psutil.Process(os.getpid())
    # Use only 2 CPU cores (adjust number as needed)
    process.cpu_affinity([0])


def main():
    if len(sys.argv) != 3:
        print("Usage: python url_predict.py <model_path> <database_path>")
        sys.exit(1)

    model_path = sys.argv[1]
    db_path = sys.argv[2]

    try:
        # Initialize handlers
        db_handler = DatabaseHandler(db_path)
        s3_handler = S3Handler()
        processor = URLImageProcessor(model_path)

        # Get next video to process
        row_id, s3_key = db_handler.get_next_unprocessed_video()

        if not row_id or not s3_key:
            logger.info("No videos to process")
            sys.exit(0)

        # Generate presigned URL
        url = s3_handler.create_presigned_url(s3_key)
        if not url:
            logger.error("Failed to generate presigned URL")
            sys.exit(1)

        # Process video and get prediction
        prediction = processor.process_url(url)

        # Update database
        if not db_handler.update_prediction(row_id, prediction):
            logger.error("Failed to update database")
            sys.exit(1)

        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    limit_cpu_usage()
    main()
