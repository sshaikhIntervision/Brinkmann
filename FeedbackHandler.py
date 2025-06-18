import os
import logging
import psycopg2
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# DB Configuration
DB_CONFIG = {
    "host": os.getenv("COSMOPG_HOST"),
    "dbname": os.getenv("COSMOPG_DBNAME"),
    "user": os.getenv("COSMOPG_USER"),
    "password": os.getenv("COSMOPG_PASSWORD"),
    "port": 5432
}

@app.route(route="FeedbackHandler")
def FeedbackHandler(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON format", status_code=400)

    try:
        # Get sessionID, messageID, feedback, feedback_text, and feedback_type details from the request
        session_id = req_body.get('sessionid')
        message_uuid = req_body.get('message_uuid')
        feedback = req_body.get('feedback')
        feedback_text = req_body.get('feedback_text')  # Optional
        feedback_type = req_body.get('feedback_type')  # Optional

        if not session_id or not message_uuid or not feedback:
            return func.HttpResponse(
                "Please provide sessionid, message_uuid, and feedback in the request.",
                status_code=400
            )

        # Call the function to update feedback
        if update_feedback(session_id, message_uuid, feedback, feedback_text, feedback_type):
            return func.HttpResponse(
                f"Feedback for sessionid: {session_id} and message_uuid: {message_uuid} updated successfully.",
                status_code=200
            )
        else:
            return func.HttpResponse(
                "Failed to update feedback. Please try again later.",
                status_code=500
            )
    
    except Exception as e:
        logging.error(f"Error in Azure Function: {str(e)}")
        return func.HttpResponse(
            "Internal server error.",
            status_code=500
        )


def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to the database: {str(e)}")
        raise e

def update_feedback(session_id, message_id, feedback, feedback_text=None, feedback_type=None):
    """Update the feedback, feedback_text, and feedback_type columns in the PostgreSQL database."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Prepare SQL query based on the provided input
        update_query = "UPDATE chat_logs SET feedback = %s"
        params = [feedback]

        # Only add feedback_text to the query if it's provided
        if feedback_text is not None:
            update_query += ", feedback_text = %s"
            params.append(feedback_text)

        # Only add feedback_type to the query if it's provided
        if feedback_type is not None:
            update_query += ", feedback_type = %s"
            params.append(feedback_type)

        # Complete the query with the WHERE clause
        update_query += " WHERE sessionid = %s AND message_uuid = %s;"
        params.extend([session_id, message_id])

        # Execute the query
        cursor.execute(update_query, tuple(params))

        # Commit the transaction
        connection.commit()

        cursor.close()
        connection.close()
        return True
    except Exception as e:
        logging.error(f"Error while updating feedback: {str(e)}")
        return False

