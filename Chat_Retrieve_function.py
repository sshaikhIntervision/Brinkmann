import logging
import json
import os
import psycopg2
import azure.functions as func
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

@app.route(route="Chat_Retrieve_function")
def Chat_Retrieve_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Chat Retrieve started.')

    try:
        # Parse incoming request
        request_body = req.get_json() if req.get_body() else None
        if not request_body:
            return func.HttpResponse(
                "Bad Request: Missing email or session_id in request body",
                status_code=400
            )

        user_email = request_body.get('email')
        session_id = request_body.get('session_id')

        if not user_email or not session_id:
            return func.HttpResponse(
                "Bad Request: Missing email or session_id in request body",
                status_code=400
            )

        # Log the input
        logging.info(f"Received email: {user_email}, session_id: {session_id}")

        # Get the database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # Query the database for records with the given email and session_id
        query = """
            SELECT * FROM chat_logs 
            WHERE email = %s AND sessionid = %s
            ORDER BY timestamp;
        """
        cursor.execute(query, (user_email, session_id))
        items = cursor.fetchall()

        # Format results as JSON
        result = []
        for item in items:
            result.append({
                "sessionId": item[12],
                "message_uuid":  item[0],
                "timestamp": str(item[1]),
                "output":  item[10],
                "Data_source":  item[3],
                "sources":  item[13],
                "email": item[5],
                "Input_query":  item[9],
                "feedback": item[6],
                "feedback_text": item[7],
                "feedback_type": item[8], 
                "document_upload":  item[4]
            })

        # Close cursor and connection
        cursor.close()
        connection.close()

        # Return the response with formatted result
        return func.HttpResponse(
            json.dumps({
                "statusCode": 200,
                "body": result
            }),
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
    
def get_db_connection():
    try:
        # Establish the connection to the database
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to the database: {str(e)}")
        raise e
