import azure.functions as func
import logging
import uuid
import requests
import json
import re
import os
import psycopg2
from azure.core.exceptions import HttpResponseError

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Database connection configuration
DB_CONFIG = {
    "host": os.getenv("COSMOPG_HOST"),
    "dbname": os.getenv("COSMOPG_DBNAME"),
    "user": os.getenv("COSMOPG_USER"),
    "password": os.getenv("COSMOPG_PASSWORD"),
    "port": 5432
}

@app.route(route="ChatTransactionHandler")
def ChatTransactionHandler(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Handling chat transactions....')

    try:
        # Parse incoming request
        request_body = req.get_json() if req.get_body() else None
        if not request_body:
            return func.HttpResponse(
                "Bad Request: Missing email or session_id in request body",
                status_code=400
            )
        
        user_email = request_body.get('email')
        query = request_body.get('query')
        followup_query = query
        doc_json = {}

        try:
            document_upload = request_body.get("document_uploaded")
        except:
            document_upload = False

        try:
            session_id = request_body.get("sessionid")
            logging.info(str(session_id))

            ### Follow up query
            if session_id is None:
                session_id = str(uuid.uuid4())
            else:
                chat_retrieve_function_url = r"https://chatretrievefunction.azurewebsites.net/api/Chat_Retrieve_function"
                headers = {'Content-Type': 'application/json'}
                followup_data = requests.post(chat_retrieve_function_url, headers=headers, data=json.dumps({"session_id": session_id, "email": user_email})).json()
                followup_query = add_followup_queries(followup_data, followup_query, prev_chat_count=2)
        except Exception as e:
            logging.info(str(e))
            session_id = str(uuid.uuid4())
        
        ### Document upload handling
        if document_upload:
            try:
                file_content = request_body.get("file_content")
                file_type = request_body.get("file_type")
                
                followup_query = add_doc_content(file_content, file_type, followup_query)
                doc_json = {"doc_content": file_content, "doc_type": file_type}
                logging.info("Doc data Added")
            except:
                logging.info("Doc follow Up")
                followup_query = add_doc_content_followup(session_id, followup_query)
                logging.info("Doc follow Up data Added")
                
        
        
        # URL of the HTTP-triggered Function
        agent_url = r"https://chatassistanthandler.azurewebsites.net/api/ChatAssistant"

        # Make a request to Agent
        headers = {'Content-Type': 'application/json'}
        assistant_response = requests.post(agent_url, headers=headers, data=json.dumps({"query": followup_query})).json()
        logging.info("Assistant response received.")

        # output_text = re.sub(r'\[doc\d+\]', 'source', output_text)
        output_text = assistant_response['content']

        if document_upload:
            assistant_response['sources'] = []
        elif (output_text == "The requested information is not available in the retrieved data. Please try another query or topic."):
            output_text = "I wasn't able to find the information you were looking. Could you try asking about something else or maybe rephrase your query? I'll be happy to assist you further."
            assistant_response['sources'] = []
        else:
            updated_data = update_dict_with_sharepoint_url(assistant_response['sources'])
            output_text = replace_references_with_links(output_text, assistant_response['sources'])



        # Update chat log to DB
        response_json = ({"message_uuid": assistant_response['message_uuid'], "input_query":query, "output": output_text, "sources": assistant_response['sources'], "sessionid": session_id,"email":user_email,"document_upload":document_upload })
        db_response_json = {**response_json, **doc_json}
        updatedb_url = r"https://updatechatlogsdb.azurewebsites.net/api/UpdateChatlogsDB"
        
        requests.post(updatedb_url, headers=headers, data=json.dumps(db_response_json))
        logging.info("Chat log updated to DB.")

        return func.HttpResponse(
            json.dumps(response_json),
            status_code=200,
            mimetype="application/json"
        )

    except HttpResponseError as e:
        logging.error(f"Error fetching data: {str(e)}")
        return func.HttpResponse(
            f"Error fetching data: {str(e)}",
            status_code=500
        )
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return func.HttpResponse(
            f"Unexpected error: {str(e)}",
            status_code=500
        )

def add_followup_queries(followup_data, query, prev_chat_count):
    try:
        # Extracting the last 2 "Input_query" and "output" values
        input_queries = [entry['Input_query'] for entry in followup_data['body']][-prev_chat_count:]
        outputs = [entry['output'] for entry in followup_data['body']][-prev_chat_count:]
        
        # Creating the dictionary with the exact desired format
        hist_result = {}
        for i in range(len(input_queries)):
            hist_result[f"input_query_{i+1}"] = input_queries[i]
            hist_result[f"output_{i+1}"] = outputs[i]

        hist_result_text = str(hist_result)
        
        ## Add it to query
        final_result = f""" 
        History chat: 
        {hist_result_text}
        
        # Find the information based on the query, 
        Also take into consideration history chats by using the last queries and outputs from the provided dictionary format.
        
        New query: {query}
        """
        
        return final_result
    except Exception as e:
        logging.info("add_followup_queries", e)


def add_doc_content(file_content, file_type, query):
    readUploadDoc_url = r"https://readuploaddoc.azurewebsites.net/api/ReadUploadDoc"
    headers = {'Content-Type': 'application/json'}
    doc_data = requests.post(readUploadDoc_url, headers=headers, data=json.dumps({"file_content": file_content, "file_type": file_type})).json()
    query = """Document text:
    """ + doc_data["Extracted Text"] + """

    Query:
    """ + query + """

    Instructions:
    Please answer the query based on the Document text."""

    return query

def add_doc_content_followup(session_id, query):
    logging.info(str(session_id))
    session_data = get_value_by_session_id(session_id)[0]
    file_content = session_data[14]
    file_type = session_data[15]
    logging.info(str(file_type))
    doc_followup_query = add_doc_content(file_content, file_type, query)
    return doc_followup_query


def get_db_connection():
    try:
        # Establish the connection to the database
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to the database: {str(e)}")
        raise e
    
def get_value_by_session_id(session_id):
    try:
        # Get a database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Build the query with session_id condition
        # query = "SELECT * FROM chat_logs WHERE sessionid = %s"
        query = "SELECT * FROM chat_logs WHERE sessionid = %s AND doc_type IS NOT NULL;"
        
        # Execute the query with session_id as a parameter
        cursor.execute(query, (session_id,))
        
        # Fetch the result
        result = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        return result
    except Exception as e:
        logging.error(f"Error while querying the database: {str(e)}")
        raise e

# Function to fetch the sharepoint_url from PostgreSQL based on the title
def fetch_sharepoint_url_from_postgresql(title):
    try:
        # Get a database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # Query the sharepoint_url from the table based on the title
        cursor.execute("SELECT sharepoint_url FROM source_url WHERE filename = %s", (title,))
        result = cursor.fetchone()

        cursor.close()
        connection.close()

        # If a result is found, return the sharepoint_url
        if result:
            return result[0]
        else:
            return None  # Return None if no matching record is found

    except Exception as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return None

# Function to update the list of dictionaries by replacing title with sharepoint_url
def update_dict_with_sharepoint_url(data):
    updated_data = []

    for item in data:
        title = item.get("title")
        if title:
            # Fetch the corresponding sharepoint_url from PostgreSQL
            sharepoint_url = fetch_sharepoint_url_from_postgresql(title)
            if sharepoint_url:
                # Replace 'title' with 'url' key and set the corresponding sharepoint_url
                item["url"] = sharepoint_url
                # del item["title"]  # Remove the title key if needed
            else:
                item["url"] = None  # Set 'url' to None if no corresponding URL is found
        updated_data.append(item)
    return updated_data

def replace_references_with_links(text, sources):
    # Find all occurrences of the pattern [docX]
    pattern = re.compile(r'\[doc(\d+)\]')
    
    def replace_match(match):
        doc_number = match.group(1)
        url = next((source['url'] for i, source in enumerate(sources) if i + 1 == int(doc_number)), None)
        if url:
            return f"[Source]({url}) "
        # return match.group(0)  # if no matching URL is found, return the original placeholder
        return ""
    
    # Replace all matches in the text
    return re.sub(pattern, replace_match, text)


