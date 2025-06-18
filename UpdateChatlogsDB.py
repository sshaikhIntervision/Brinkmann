import azure.functions as func
import logging
import psycopg2
import json
import logging
from psycopg2 import sql
import uuid
import datetime
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

DB_CONFIG = {
    "host": os.getenv("COSMOPG_HOST"),
    "dbname": os.getenv("COSMOPG_DBNAME"),
    "user": os.getenv("COSMOPG_USER"),
    "password": os.getenv("COSMOPG_PASSWORD"),
    "port": 5432
}

@app.route(route="UpdateChatlogsDB")
def UpdateChatlogsDB(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Chatlog Updating....')
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON format", status_code=400)

    # Log the incoming object
    # logging.info(f"Received object: {json.dumps(req_body)}")

    # Get DB connection
    try:
        connection = get_db_connection()

        # Store the object in the database
        store_object_in_db(connection, req_body)
        
        # Close DB connection
        connection.close()
        logging.info('Chatlog Updated to DB')

        return func.HttpResponse("Object stored successfully in Cosmos DB PostgreSQL!", status_code=200)
    
    except Exception as e:
        logging.error(f"Error while processing request: {str(e)}")
        return func.HttpResponse("An error occurred while storing the object.", status_code=500)

    
def get_db_connection():
    try:
        # Establish the connection to the database
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to the database: {str(e)}")
        raise e

def store_object_in_db(connection, obj_data):
    try:
        # Create a cursor object to interact with the DB
        cursor = connection.cursor()
        
        # SQL query to insert data (ensure the table and columns match your DB schema)
        insert_query = sql.SQL("""
            INSERT INTO chat_logs (
                message_uuid, timestamp, chat_summary, data_source, document_upload,
                email, feedback, feedback_text, feedback_type, input_query, output, 
                processed_query, sessionid, sources, doc_content, doc_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        # Extract relevant data from the object
        message_uuid = obj_data.get('message_uuid',str(uuid.uuid4()))  # Generate a unique UUID for the message
        timestamp = obj_data.get('timestamp', datetime.datetime.now().isoformat())  # Default to current time if not provided
        chat_summary = obj_data.get('chat_summary')
        data_source = obj_data.get('data_source')
        document_upload = obj_data.get('document_upload')
        email = obj_data.get('email')
        feedback = obj_data.get('feedback')
        feedback_text = obj_data.get('feedback_text')
        feedback_type = obj_data.get('feedback_type')
        input_query = obj_data.get('input_query')
        output = obj_data.get('output')
        processed_query = obj_data.get('processed_query')
        sessionid = obj_data.get('sessionid', str(uuid.uuid4()))
        sources = json.dumps(obj_data.get('sources', {}))
        doc_content = obj_data.get('doc_content')
        doc_type = obj_data.get('doc_type')

        # Data tuple to insert
        data = (
            message_uuid, timestamp, chat_summary, data_source, document_upload,
            email, feedback, feedback_text, feedback_type, input_query, output,
            processed_query, sessionid, sources, doc_content, doc_type
        )

        # Execute the SQL query
        cursor.execute(insert_query, data)
        connection.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Error while inserting data into the DB: {str(e)}")
        raise e
