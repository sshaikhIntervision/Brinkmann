import azure.functions as func
import logging
import requests
import json
from msal import ConfidentialClientApplication
from json_repair import repair_json
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import os
import psycopg2

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Microsoft Graph API Details
CLIENT_ID = os.getenv('AD_CLIENT_id')
CLIENT_SECRET = os.getenv('AD_CLIENT_SECRET')
TENANT_ID = os.getenv('TENANT_ID')

# PostgreSQL Configuration
DB_HOST = os.getenv('COSMOPG_HOST')
DB_PORT = 5432  # Default port for PostgreSQL
DB_USER = os.getenv('COSMOPG_USER')
DB_PASSWORD = os.getenv('COSMOPG_PASSWORD')
DB_NAME = os.getenv('COSMOPG_DBNAME')

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')
FOLDER = "scraped_pages"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
SITE_ID = "askbrinkmann.sharepoint.com,9016808e-d23f-4386-9ef9-e0d5d635bb79,a4630e11-fe5d-4114-940f-d5196ee016b1"
LIST_TITLE = "Site Pages"
GRAPH_API_URL = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_TITLE}/items"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

EXCLUDE_URLS = [
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Documents.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/search.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Welcome2.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Templates",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Templates/News.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Templates/Orion-News.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Pre-Con.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Prepare-for-2023-Annual-Performance-Reviews.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Moving-On-Up--Brinkmann-Reaches--80-on-ENR-s-Top-400-General-Contractor-List.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Brinkmann-Labor-Rates.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Lifelong-Learning-in-Action--Cold-Storage-Industrial-Business-Unit-Tour-Evapco-World-Headquarters.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Missouri-University-of-Science-and-Technology-Amusement-Park-Design-Camp-Visits-Oasis-at-Lakeport-Project.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Now-Available--Brinkmann-s-Digital-Project-List.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Header-Test.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Occupational%20Safety%20and%20Health%20Administration%20(OSHA)%20Inspections.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Brinkmann%20Brag.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Brinkmann%20Bravo.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Jobsite%20Utility%20Setup.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Congratulations--Aubrey-Wyrick-.aspx",
      "https://askbrinkmann.sharepoint.com/sites/Operations/SitePages/Now-Live--Project-Lifecycle.aspx"
  ]


@app.route(route="Sharpoint_Scrape_Sites")
def Sharpoint_Scrape_Sites(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Connecting to Sharepoint...')
    try:
        result = process_sharepoint_pages()
        return func.HttpResponse("Sites Scraped and Saved to Blob Storage.", status_code=200)
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(str(e), status_code=500)
    

def get_access_token():
    """Get an access token for Microsoft Graph API"""
    app = ConfidentialClientApplication(CLIENT_ID, CLIENT_SECRET, authority=AUTHORITY)
    token_response = app.acquire_token_for_client(scopes=SCOPE)

    if "access_token" in token_response:
        return token_response["access_token"]
    else:
        raise Exception(f"Error fetching token: {token_response.get('error_description')}")


def fetch_sharepoint_page(etag_id):
    """Fetch SharePoint Page content"""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/pages/{etag_id}/microsoft.graph.sitePage?$expand=canvasLayout"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        cleaned_text = repair_json(response.text)
        try:
            return json.loads(cleaned_text)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}")
            return None
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
        return None


def save_content_locally(directory, file_name, content):
    """Save formatted content to a local file."""
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, file_name)

    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(content)
        logging.info("Content successfully saved locally as %s.", file_path)
    except Exception as e:
        logging.error("Failed to save content locally: %s", e)

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


def save_to_blob(directory, file_name, content, sharepoint_url):
    """Save content to Azure Blob Storage."""
    try:
        file_path = os.path.join(directory, file_name)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=file_path)
        blob_client.upload_blob(content, overwrite=True)
        store_in_postgresql(file_name, file_path, sharepoint_url)

        logging.info("File successfully saved to Blob Storage: %s", file_name)
    except Exception as e:
        logging.error("Failed to save content to Blob: %s", e)


def format_html_content(html_content):
    """Format HTML content for readability."""
    soup = BeautifulSoup(html_content, "html.parser")
    formatted_content = []

    for element in soup.descendants:
        if element.name == "a":
            formatted_content.append(f"[{element.get_text(strip=True)}]({element.get('href', '')})")
        elif element.name == "table":
            for row in element.find_all("tr"):
                cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                formatted_content.append(" | ".join(cells))
        elif element.name in ["p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol"]:
            formatted_content.append(element.get_text(strip=True))

    return "\n".join(formatted_content)


def process_sharepoint_pages():
    """Fetch and process pages from SharePoint."""
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = requests.get(GRAPH_API_URL, headers=headers)

    if response.status_code != 200:
        logging.error(f"Error {response.status_code}: {response.text}")
        return

    for item in response.json().get('value', []):
        web_url = item.get('webUrl')

        # Skip URLs that are in the exclude list
        if web_url in EXCLUDE_URLS:
            logging.info(f"Skipping excluded URL: {web_url}")
            continue

        etag = item.get('eTag', '').strip('"').split(',')[0]
        logging.info(f"Fetching Page ID: {etag} - URL: {web_url}")

        page_response = fetch_sharepoint_page(etag)
        if page_response:
            horizontal_sections = page_response.get("canvasLayout", {}).get("horizontalSections", [])
            all_formatted_content = []

            for section in horizontal_sections:
                for column in section.get("columns", []):
                    for webpart in column.get("webparts", []):
                        formatted_content = format_html_content(webpart.get("innerHtml", ""))
                        all_formatted_content.append(formatted_content)
                        contacts = webpart.get("data", {}).get("properties", {}).get("persons", [])
                        if contacts:
                            all_formatted_content.append(str(contacts))

            combined_content = "\n".join(all_formatted_content)
            file_name = f"{web_url.split('/')[-1].replace('%20', '').replace('%26', '').replace('.aspx', '.txt')}"

            # save_content_locally(FOLDER, file_name, combined_content)
            save_to_blob(FOLDER, file_name, combined_content, web_url)
        else:
            logging.error(f"Failed to fetch page: {web_url}")