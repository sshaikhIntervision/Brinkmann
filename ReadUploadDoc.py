import logging
import base64
import tempfile
import os
from PyPDF2 import PdfReader
from docx import Document
import azure.functions as func
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="ReadUploadDoc")
def ReadUploadDoc(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Extracting Text...')
    """Azure Function to process base64-encoded files and extract text."""
    try:
        # Get JSON payload from the request
        req_body = req.get_json()

        # Extract the file data and type from the request body
        encoded_data = req_body.get('file_content')
        file_type = req_body.get('file_type', '').lower()

        if not encoded_data or not file_type:
            return func.HttpResponse(
                "Invalid input. Provide 'file' and 'file_type'.", 
                status_code=400
            )

        # Decode the base64 string
        file_data = base64.b64decode(encoded_data)

        # Create a temporary file to save the uploaded file
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_type}") as temp_file:
            temp_file.write(file_data)
            temp_file_path = temp_file.name

        # Read the file content based on its type
        if file_type == 'txt':
            extracted_text = read_txt(temp_file_path)
        elif file_type == 'docx':
            extracted_text = read_docx(temp_file_path)
        elif file_type == 'pdf':
            extracted_text = read_pdf(temp_file_path)
        else:
            os.remove(temp_file_path)
            return func.HttpResponse(
                "Unsupported file type. Only 'pdf', 'docx', and 'txt' are supported.",
                status_code=400
            )

        # Clean up the temporary file
        os.remove(temp_file_path)
        logging.info('Extraction Done.')
        # Return the extracted text as a response
        return func.HttpResponse(
            json.dumps({"Extracted Text": extracted_text}),
            mimetype="text/plain",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        return func.HttpResponse(
            f"An error occurred: {str(e)}", 
            status_code=500
        )



def read_txt(file_path):
    """Read text from a plain text file."""
    with open(file_path, 'r') as f:
        return f.read()

def read_docx(file_path):
    """Read text from a DOCX file."""
    doc = Document(file_path)
    return '\n'.join([paragraph.text for paragraph in doc.paragraphs])

def read_pdf(file_path):
    """Read text from a PDF file using PyPDF2."""
    reader = PdfReader(file_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text()
    return text
