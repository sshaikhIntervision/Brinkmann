import azure.functions as func
import os
import logging
import requests
from msal import ConfidentialClientApplication
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import psycopg2

app = func.FunctionApp()

# Your existing configurations
site_ids = {
    "operations": "askbrinkmann.sharepoint.com,9016808e-d23f-4386-9ef9-e0d5d635bb79,a4630e11-fe5d-4114-940f-d5196ee016b1"
}

# Azure AD credentials
client_id = os.getenv('AD_CLIENT_id')
client_secret = os.getenv('AD_CLIENT_SECRET')
tenant_id = os.getenv('TENANT_ID')

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

# PostgreSQL Configuration
DB_HOST = os.getenv('COSMOPG_HOST')
DB_PORT = 5432  # Default port for PostgreSQL
DB_USER = os.getenv('COSMOPG_USER')
DB_PASSWORD = os.getenv('COSMOPG_PASSWORD')
DB_NAME = os.getenv('COSMOPG_DBNAME')

# Excluded file extensions
EXCLUDED_EXTENSIONS = ['.mp4', '.mov', '.avi', '.mp3', '.wav', '.flac', '.mkv', '.png', '.jpg', '.msg', '.m4v', '.eps', '.jpeg', '.jfif', '.heic']

# Avoid file names containing specific keywords (case-insensitive)
AVOID_LIST = ["confidential", "offer letter", "compensation", 'Termination']

access_token = None
token_expiry = 0
token_lock = threading.Lock()

session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

@app.route(route="sharepointPlugin")
def sharepointPlugin(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Connecting to Sharepoint...')
    try:
        result = extract_sharepoint()
        return func.HttpResponse(result, status_code=200)
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(str(e), status_code=500)

def get_access_token():
    """Fetch a new access token from Azure AD."""
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    result = app.acquire_token_for_client(scopes=scopes)

    if "access_token" in result:
        global token_expiry
        token_expiry = time.time() + 3600
        return result["access_token"]
    else:
        raise Exception("Failed to acquire access token")


def refresh_access_token():
    """Refresh the global access token."""
    global access_token
    with token_lock:
        access_token = get_access_token()


def get_valid_access_token():
    """Retrieve a valid access token, refreshing it if necessary."""
    global access_token, token_expiry
    with token_lock:
        if access_token is None or time.time() > token_expiry - 300:
            refresh_access_token()
        return access_token


def fetch_all_drives(site_id, access_token):
    """Fetch all drive IDs for a given SharePoint site."""
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch drives for site {site_id}: {response.json()}")

    drives = response.json().get("value", [])
    return [drive["id"] for drive in drives]


def is_excluded_file(file_name, folder_name):
    """Check if a file has an excluded extension or contains avoided keywords."""
    file_extension = os.path.splitext(file_name)[1].lower()
    if file_extension in EXCLUDED_EXTENSIONS:
        return True

    for keyword in AVOID_LIST:
        if keyword.lower() in folder_name.lower() or keyword.lower() in file_name.lower():
            return True
    return False


def upload_to_blob_storage(file_url, blob_name, filename, sharepoint_url):
    """Download file from SharePoint and upload to Azure Blob Storage and store in PostgreSQL."""
    try:
        # Download File with Timeout
        response = session.get(file_url, stream=True, timeout=30)
        response.raise_for_status()  # Raise error for bad status codes

        # Read file into memory
        file_data = BytesIO(response.content)

        # Upload to Azure Blob Storage
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(file_data, overwrite=True, content_settings=ContentSettings(content_type="application/octet-stream"))

        # Store details in PostgreSQL
        store_in_postgresql(filename, blob_name, sharepoint_url)

        logging.info(f"Uploaded: {blob_name}")
    except requests.exceptions.RequestException as e:
        logging.info(f"Request error for {file_url}: {e}")
    except Exception as e:
        logging.info(f"Error processing {file_url}: {e}")


def store_in_postgresql(filename, blob_name, sharepoint_url):
    """Store file information in PostgreSQL table (insert or update)."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cursor = conn.cursor()

        # Insert into source_url table or update if the filename already exists
        cursor.execute("""
            INSERT INTO source_url (filename, blobname, sharepoint_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (filename)
            DO UPDATE SET blobname = EXCLUDED.blobname, sharepoint_url = EXCLUDED.sharepoint_url;
        """, (filename, blob_name, sharepoint_url))

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Stored or updated file info: {filename}, {blob_name}, {sharepoint_url}")
    except Exception as e:
        logging.error(f"Error storing data in PostgreSQL: {e}")


def fetch_drive_content(drive_id, folder_path="", site_name=""):
    """Fetch content of a SharePoint drive recursively."""
    headers = {"Authorization": f"Bearer {get_valid_access_token()}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children" if folder_path == "" else \
          f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"

    try:
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.info(f"Failed to fetch content for drive {drive_id}: {e}")
        return

    items = response.json().get("value", [])
    threads = []

    for item in items:
        if "folder" in item:
            fetch_drive_content(drive_id, f"{folder_path}/{item['name']}".strip("/"), site_name)
        else:
            if not is_excluded_file(item["name"], folder_path):
                blob_name = f"{site_name}/{folder_path}/{item['name']}".replace("\\", "/")
                sharepoint_url = item["@microsoft.graph.downloadUrl"]
                shareable_link = file_weblink(drive_id, item['id'])
                logging.info(str(blob_name))
                thread = threading.Thread(target=upload_to_blob_storage, args=(sharepoint_url, blob_name, item["name"], shareable_link))
                thread.start()
                threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


def extract_sharepoint():
    """Extract SharePoint files and upload to Azure Blob Storage."""
    logging.info("Started Fetching")
    refresh_access_token()
    for site_name, site_id in site_ids.items():
        try:
            drive_ids = fetch_all_drives(site_id, get_valid_access_token())
            for drive_id in drive_ids:
                fetch_drive_content(drive_id, site_name=site_name)
        except Exception as e:
            logging.info(f"Error processing site {site_name}: {e}")
    logging.info("Process Done")
    return "Files uploaded to Azure Blob Storage"


def file_weblink(DRIVE_ID, ITEM_ID):
    # API endpoint to create a sharing link
    urll = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}/createLink"

    header = {"Authorization": f"Bearer {get_valid_access_token()}", "Content-Type": "application/json"}


    data = {
        "type": "view",  # "edit" for edit permissions
        "scope": "organization"  # "organization" for internal users only
    }

    respons = requests.post(urll, headers=header, json=data)

    if respons.status_code == 200:
        link = respons.json().get("link").get("webUrl")
        # logging.info(f"Shareable link: {link}")
    else:
        logging.info(f"Error: {respons.json()}")
    return link