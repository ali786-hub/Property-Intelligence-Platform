import os
import glob
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Silence the extremely verbose Azure HTTP logs
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Load Azure credentials
load_dotenv()

# Default local directory for the test files
LOCAL_SOURCE_FOLDER = r"C:\Omnijourney_Kofking_github\data\landing_zone"
CONTAINER_NAME = "landingzone"

def simulate_api_stream(local_dir: str):
    """
    Simulates a producer API by streaming raw CSV files to Azure.
    Uses the native Azure Blob SDK for maximum stability and automatic retries 
    on slow/home internet connections.
    """
    logging.info(f"📡 API Simulator: Streaming data to Azure using Native SDK...")
    
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
    if not account_name or not account_key:
        logging.error("Missing Azure credentials in .env file.")
        return

    try:
        # Connect to the Azure Storage Account
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    except Exception as e:
        logging.error(f"Could not initialize Azure client: {e}")
        return

    csv_files = glob.glob(os.path.join(local_dir, "*.csv"))
    if not csv_files:
        logging.info(f"No CSV files found in {local_dir}. Nothing to upload.")
        return

    success_count = 0
    skip_count = 0

    # Single-threaded but highly resilient upload loop
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        blob_client = container_client.get_blob_client(filename)

        try:
            # Check if file exists to prevent duplicate uploads
            if blob_client.exists():
                logging.info(f"⏭️ SKIP: '{filename}' already exists in Azure. Deleting local copy.")
                os.remove(file_path)
                skip_count += 1
                continue
            
            logging.info(f"📤 Uploading {filename} (this may take a moment depending on your bandwidth)...")
            
            # The native SDK automatically handles chunking and retries!
            with open(file_path, "rb") as data:
                blob_client.upload_blob(
                    data, 
                    overwrite=False, 
                    max_concurrency=2,  # Safe, low concurrency to avoid router congestion
                    connection_timeout=14400  # Extremely generous timeout for slow connections
                )
            
            # Delete local file on guaranteed success
            os.remove(file_path)
            logging.info(f"✅ Uploaded and deleted locally: {filename}")
            success_count += 1
            
        except ResourceExistsError:
            logging.info(f"⏭️ SKIP: '{filename}' was uploaded by another process. Deleting local copy.")
            os.remove(file_path)
            skip_count += 1
        except Exception as e:
            logging.error(f"❌ Failed to upload {filename}: {e}")

    logging.info(f"🎉 API Simulation complete. {success_count} uploaded, {skip_count} skipped.")

if __name__ == "__main__":
    simulate_api_stream(LOCAL_SOURCE_FOLDER)
