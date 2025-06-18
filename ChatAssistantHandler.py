import azure.functions as func
import logging
from openai import AzureOpenAI
import json
import os


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
AZURE_SEARCH_INDEX = os.getenv("AZURE_SEARCH_INDEX")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY")
DEPLOYMENT = os.getenv("DEPLOYMENT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")

@app.route(route="ChatAssistant")
def ChatAssistant(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    query = req.params.get('query')
    if not query:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            query = req_body.get('query')
    
    
    ai_output = query_construction_bot(query)
    ai_output_dict = json.loads(ai_output)

    result = {
            "message_uuid": ai_output_dict["id"],
            "content": ai_output_dict["choices"][0]["message"]["content"],
            "sources": [
                {
                    "content": citation["content"],
                    "title": citation["title"],
                    "url": citation["url"]
                }
                for citation in ai_output_dict["choices"][0]["message"]["context"]["citations"]
            ]
        }

    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype="application/json"
    )


def query_construction_bot(user_query: str):

    PROMPT = """
    You are an expert assistant trained to answer queries related to construction projects. Your task is to provide accurate and detailed responses regarding documents related to the project, labor, equipment, materials, and other aspects involved in the construction process.

    Respond to questions that may include but are not limited to:

    1. **Construction Documents:** Plans, blueprints, permits, specifications, and any legal or regulatory paperwork.
    2. **Project Information:** Timelines, schedules, progress reports, and project scope details.
    3. **Labor:** Workforce requirements, labor agreements, roles and responsibilities, and safety guidelines.
    4. **Equipment:** Availability, specifications, usage, maintenance, and rental information.
    5. **Materials:** Types of materials used, quantities, suppliers, procurement processes, and costs.
    6. **Standards and Compliance:** Adherence to industry standards, building codes, safety regulations, and environmental requirements.

    **Important Guidelines:**
    - Try to search first if the filename related to query exists.
    - Give first preference to scraped files and then also find in other docs.
    - If asked about WUTS, assume user is asking about Morning Huddle & Warm Up to Safety (WUTS)
    - If Super is mentioned then it actually means Superintendent and NOT supervisor

    Ensure that your responses are clear, concise, and easy to understand, making sure to reference relevant documents or provide appropriate instructions when necessary. 
    If additional clarification is needed for a query, offer to assist further.

    """
    # **Important Guidelines:**

    # 1. **Handling Personally Identifiable Information (PII):**  
    #    If a user asks for or provides any personal information (e.g., names, addresses, contact details, employee identifiers), *do not process or share* such details. Respond by gently reminding users not to share sensitive personal data and advise them to reach out through appropriate secure channels for such inquiries. 

    # 2. **Unsafe or Inappropriate Language:**  
    #    If a user uses offensive or inappropriate language, calmly redirect the conversation back to the topic at hand and request they maintain a professional tone. Do not engage with or promote unsafe or harmful behavior. If the language persists, politely suggest that they reframe their inquiry.


    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION
    )

    completion = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=[
            {"role": "system", "content": PROMPT},
            {"role": "user", "content": user_query},
        ],
        extra_body={
            "data_sources": [
                {
                    "type": "azure_search",
                    "parameters": {
                        "endpoint": AZURE_SEARCH_ENDPOINT,
                        "index_name": AZURE_SEARCH_INDEX,
                        "authentication": {
                            "type": "api_key",
                            "key": AZURE_SEARCH_API_KEY
                        }
                    }
                }
            ]
        }
    )

    return completion.model_dump_json(indent=2)
