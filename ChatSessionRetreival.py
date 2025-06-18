import azure.functions as func
from azure.functions import HttpRequest, HttpResponse
import json
import os
import psycopg2
import logging
import datetime


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

DB_CONFIG = {
    "host": os.getenv("COSMOPG_HOST"),
    "dbname": os.getenv("COSMOPG_DBNAME"),
    "user": os.getenv("COSMOPG_USER"),
    "password": os.getenv("COSMOPG_PASSWORD"),
    "port": 5432
}


@app.route(route="ChatSessionRetreival")
def ChatSessionRetreival(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Requesting session...')

    # Get the email from the request body
    try:
        req_body = req.get_json()
        user_email = req_body['email']
    except ValueError:
        return HttpResponse("Invalid request body", status_code=400)

    logging.info(f"Received request for user: {user_email}")

    try:
        # Initialize the database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # Query the chat history based on the user email
        cursor.execute("""
            SELECT sessionid, timestamp, input_query 
            FROM chat_logs 
            WHERE email = %s
            ORDER BY timestamp DESC
        """, (user_email,))

        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        if not rows:
            return HttpResponse(
            json.dumps([]),
            status_code=200,
            mimetype="application/json"
        )
            # return HttpResponse(f"No chat history found for {user_email}", status_code=404)

        # Process the results and filter the latest query per session
        latest_messages = {}
        for row in rows:
            session_id = row[0]
            timestamp = row[1]  # This could be a datetime object or string
            input_query = row[2]

            # If timestamp is a datetime object, convert it to ISO format
            if isinstance(timestamp, datetime.datetime):
                timestamp_str = timestamp.isoformat()
            else:
                timestamp_str = str(timestamp)  # If it's already a string, use it directly


            if session_id not in latest_messages or datetime.datetime.fromisoformat(latest_messages[session_id]['timestamp']) < timestamp:
                latest_messages[session_id] = {
                    'sessionId': session_id,
                    'timestamp': timestamp_str,  # Use the ISO string instead of datetime object
                    'Input_query': input_query
                }

        # Prepare the sorted results by timestamp
        sorted_messages = sorted(latest_messages.values(), key=lambda x: x['timestamp'], reverse=True)

        return HttpResponse(
            json.dumps(sorted_messages),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error processing the request: {str(e)}")
        return HttpResponse(f"Error fetching data: {str(e)}", status_code=500)

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to the database: {str(e)}")
        raise e
