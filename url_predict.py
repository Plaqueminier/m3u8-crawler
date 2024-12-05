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
                SELECT id, key
                FROM videos
                WHERE prediction IS NULL
                   OR prediction = ?
                ORDER BY id DESC
                LIMIT 1
            """,
                ("0" * 100,),
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
    def __init__(self, pretrained=False):
        super(TransferModel, self).__init__()
        self.resnet = models.resnet18(pretrained=pretrained)

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

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        else:
            shutil.rmtree(output_dir)
            os.makedirs(output_dir)

        try:
            # Get video duration first
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

            self.logger.info("Getting video duration...")
            duration_output = subprocess.run(
                duration_cmd, capture_output=True, text=True
            )
            duration = float(duration_output.stdout.strip())

            self.logger.info(f"Video duration: {duration:.2f} seconds")

            # Calculate frame interval to get exactly 100 frames
            frame_interval = (
                duration / 100
            )  # This will give us 100 evenly spaced frames

            # Create progress bar
            with tqdm(total=100, desc="Extracting frames") as pbar:
                # Single FFmpeg command to extract all frames at once
                cmd = [
                    "ffmpeg",
                    "-i",
                    url,
                    "-vf",
                    f"fps=1/{frame_interval}",  # Extract frames at calculated interval
                    "-frame_pts",
                    "1",
                    "-vsync",
                    "0",
                    "-frames:v",
                    "100",  # Limit to exactly 100 frames
                    "-threads",
                    "1",  # Limit thread usage
                    "-preset",
                    "ultrafast",  # Use fastest encoding preset
                    "-f",
                    "image2",
                    "-qscale:v",
                    "2",  # High quality
                    os.path.join(output_dir, f"frame_%03d.{format}"),
                ]

                # Run FFmpeg with progress monitoring
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )

                # Update progress bar while FFmpeg runs
                while process.poll() is None:
                    # Count current frames
                    current_frames = len(
                        [
                            f
                            for f in os.listdir(output_dir)
                            if f.lower().endswith(f".{format}")
                        ]
                    )
                    pbar.n = min(100, current_frames)
                    pbar.refresh()
                    time.sleep(0.1)

                # Ensure progress bar reaches 100%
                pbar.n = 100
                pbar.refresh()

            # Verify we got exactly 100 frames
            frames = sorted(
                [f for f in os.listdir(output_dir) if f.lower().endswith(f".{format}")]
            )
            frames_saved = len(frames)

            if frames_saved != 100:
                self.logger.warning(
                    f"Expected 100 frames but got {frames_saved} frames"
                )

                # If we got more than 100 frames, keep only the first 100
                if frames_saved > 100:
                    for frame in frames[100:]:
                        os.remove(os.path.join(output_dir, frame))
                    frames_saved = 100
                    self.logger.info("Removed excess frames")

                # If we got less than 100 frames, duplicate the last frame
                elif frames_saved < 100:
                    last_frame = frames[-1]
                    for i in range(frames_saved + 1, 101):
                        shutil.copy2(
                            os.path.join(output_dir, last_frame),
                            os.path.join(output_dir, f"frame_{i:03d}.{format}"),
                        )
                    frames_saved = 100
                    self.logger.info("Duplicated last frame to reach 100 frames")

            self.logger.info(
                f"Successfully prepared {frames_saved} frames in {output_dir}"
            )
            return frames_saved

        except subprocess.CalledProcessError as e:
            self.logger.error(f"FFmpeg error: {str(e)}")
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")

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
        checkpoint = torch.load(model_path, map_location=self.device)

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
                    (224, 224), interpolation=transforms.InterpolationMode.BILINEAR
                ),
                transforms.ToTensor(),
                transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
            ]
        )

        self.frame_extractor = VideoFrameExtractor()
        self.batch_size = 16  # Process images in batches

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

    @torch.no_grad()  # Ensure no gradients are computed
    def predict_batch(self, image_paths: List[str]) -> List[float]:
        """Predict a batch of images at once"""
        try:
            # Prepare batch
            batch_tensors = []
            for img_path in image_paths:
                image = Image.open(img_path).convert("RGB")
                image_tensor = self.transform(image)
                batch_tensors.append(image_tensor)

            # Stack tensors and move to device
            batch = torch.stack(batch_tensors).to(self.device)

            # Run inference
            outputs = self.model(batch)
            probabilities = outputs.squeeze().cpu().numpy().tolist()

            # Handle single-item case
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
    main()
