import os
import psycopg2
import logging
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("COSMOPG_HOST"),
    "dbname": os.getenv("COSMOPG_DBNAME"),
    "user": os.getenv("COSMOPG_USER"),
    "password": os.getenv("COSMOPG_PASSWORD"),
    "port": 5432
}

@app.route(route="DeleteChatHandler")
def DeleteChatHandler(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Delete entry request..')

    request_body = req.get_json() if req.get_body() else None
    if not request_body:
        return func.HttpResponse(
            "Bad Request: Missing email or session_id in request body",
            status_code=400
            )
        
    user_email = request_body.get('email')
    session_id = request_body.get('sessionid')
    if not session_id:
        return func.HttpResponse(
            "Please provide a sessionid in the request.",
            status_code=400
        )

    try:
        # Get a connection to the database
        connection = get_db_connection()
        cursor = connection.cursor()

        # SQL query to delete records with the given session_id
        delete_query = "DELETE FROM chat_logs WHERE sessionid = %s AND email = %s"
        cursor.execute(delete_query, (session_id, user_email))

        # Commit the changes to the database
        connection.commit()

        # Check how many rows were affected
        rows_deleted = cursor.rowcount
        cursor.close()
        connection.close()
        logging.info('Entry Deleted')

        # Return response based on deletion result
        if rows_deleted > 0:
            return func.HttpResponse(
                f"Successfully deleted {rows_deleted} record(s) with sessionid = {session_id} and email = {user_email}.",
                status_code=200
            )
        else:
            return func.HttpResponse(
                f"No records found with sessionid = {session_id} and email = {user_email}.",
                status_code=404
            )

    except Exception as e:
        logging.error(f"Error during database operation: {str(e)}")
        return func.HttpResponse(
            "Internal Server Error. Please try again later.",
            status_code=500
        )

# Function to establish DB connection
def get_db_connection():
    try:
        # Establish the connection to the database
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to the database: {str(e)}")
        raise e