import os
import sys
import subprocess
import zipfile
import shutil
import argparse
import logging
from pathlib import Path
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_args():
    parser = argparse.ArgumentParser(description="Sync Python packages from ProGet to Snowflake Stage")
    parser.add_argument("--requirements", default="requirements.txt", help="Path to requirements.txt")
    parser.add_argument("--stage", required=True, help="Snowflake stage name (e.g., @MY_DB.MY_SCHEMA.MY_STAGE)")
    parser.add_argument("--download-dir", default="./downloaded_packages", help="Directory to download packages to")
    parser.add_argument("--zip-name", default="app_packages.zip", help="Name of the output zip file")
    
    # ProGet credentials (often handled via pip.conf or env vars, but can be passed here if needed)
    parser.add_argument("--proget-url", help="ProGet repository URL")
    
    return parser.parse_args()

def download_packages(requirements_path: str, download_dir: str, index_url: Optional[str] = None):
    """
    Download packages listed in requirements.txt using pip.
    """
    logger.info(f"Downloading packages from {requirements_path} to {download_dir}...")
    
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir)

    cmd = [
        sys.executable, "-m", "pip", "download",
        "-r", requirements_path,
        "-d", download_dir,
        "--platform", "manylinux2014_x86_64", # Snowflake UDFs run on Linux
        "--only-binary=:all:", # Prefer wheels
        "--python-version", "3.8", # Adjust to match Snowflake Python version (3.8, 3.9, 3.10, 3.11)
    ]

    if index_url:
        cmd.extend(["--index-url", index_url])
        
    try:
        subprocess.check_call(cmd)
        logger.info("Download complete.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to download packages: {e}")
        raise

def create_zip(source_dir: str, output_filename: str):
    """
    Zip all files in the source directory.
    """
    logger.info(f"Zipping packages in {source_dir} to {output_filename}...")
    
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)
                
    logger.info("Packaging complete.")

def upload_to_snowflake(zip_file_path: str, stage_name: str):
    """
    Upload the zip file to Snowflake stage.
    """
    try:
        import snowflake.connector
    except ImportError:
        logger.error("snowflake-connector-python is required. Please install it.")
        sys.exit(1)

    logger.info(f"Connecting to Snowflake to upload {zip_file_path} to {stage_name}...")

    # Get credentials from environment variables
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    role = os.environ.get("SNOWFLAKE_ROLE")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")

    if not all([account, user, password]):
        logger.error("Missing Snowflake credentials in environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD).")
        raise ValueError("Missing Snowflake credentials")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    try:
        cursor = conn.cursor()
        
        # Use PUT command to upload
        put_cmd = f"PUT file://{os.path.abspath(zip_file_path)} {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        logger.info(f"Executing: {put_cmd}")
        
        cursor.execute(put_cmd)
        
        result = cursor.fetchall()
        for row in result:
            logger.info(f"Upload result: {row}")
            
        logger.info("Upload to Snowflake stage complete.")
        
    finally:
        conn.close()

def main():
    args = setup_args()
    
    try:
        # 1. Download
        download_packages(args.requirements, args.download_dir, args.proget_url)
        
        # 2. Zip
        create_zip(args.download_dir, args.zip_name)
        
        # 3. Upload
        upload_to_snowflake(args.zip_name, args.stage)
        
        # Cleanup
        if os.path.exists(args.download_dir):
            shutil.rmtree(args.download_dir)
        if os.path.exists(args.zip_name):
            os.remove(args.zip_name)
            
        logger.info("Pipeline finished successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

