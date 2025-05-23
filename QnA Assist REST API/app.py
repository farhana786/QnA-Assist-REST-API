import os
import re
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from openai import AzureOpenAI
from dotenv import load_dotenv
from prompts import get_system_prompt
from snowflake_utils import get_snowflake_connection

load_dotenv()

app = FastAPI()

# Establish OpenAI connection
try:
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),  
        api_version="2024-05-01-preview",
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        )
except Exception as e:
    st.error(f"Failed to establish OpenAI connection: {e}")

deployment_name=os.getenv("DEPLOYMENT_NAME")

class UserPrompt(BaseModel):
    prompt: str

@app.post("/query")
async def query_openai(prompt: UserPrompt):
    system_message = {"role": "system", "content": get_system_prompt()}
    user_message = {"role": "user", "content": prompt.prompt}
    
    messages = [system_message, user_message]

    response = ""
    for delta in client.chat.completions.create(
        model=deployment_name,
        messages=messages,
        stream=True,
    ):
        if delta.choices:
            response += (delta.choices[0].delta.content or "")

    sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
    
    if sql_match:
        sql = sql_match.group(1)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            result_dict = [dict(zip(column_names, row)) for row in results]
            return {"data": result_dict}
            #return {"results": results, "query": sql}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"SQL execution error: {e}")
    else:
        return {"response": response}