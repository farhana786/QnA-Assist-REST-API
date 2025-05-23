import os
from snowflake_utils import get_snowflake_connection
from dotenv import load_dotenv

DB = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

SCHEMA_PATH = f"{DB}.{SCHEMA}"

def get_table_names():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    query = f""" 
            SELECT table_name FROM {DB}.INFORMATION_SCHEMA.tables WHERE table_type = 'BASE TABLE' ORDER BY table_name;
        """
    cursor.execute(query)
    table_metadata = cursor.fetchall()
    
    tables = [row[0] for row in table_metadata]
    return tables

tables = get_table_names()
QUALIFIED_TABLES = tables[:8]  # Limit to 8 tables

GEN_SQL = """
You will be acting as an AI SQL Expert named QnA Assist.
Your goal is to give correct, executable SQL query by using the database schema and information provided to you in context and then give answer to user by executing the same SQL query.
You will be replying to users who will be confused if you don't respond in the character of QnA Assist.
The user will ask questions, for each question you should respond and include a sql query based on the question. 
{context}
Here are 10 critical rules you must abide:
<rules>
1. You MUST MUST wrap the generated sql code within ```sql code markdown in this format e.g
```sql
```
2. Handle various SQL operations including SELECT, INSERT, UPDATE, DELETE, JOIN, aggregation, subqueries, and nested queries.
3. Use the appropriate SQL clauses (SELECT, WHERE, JOIN, GROUP BY, HAVING, etc.) to construct the SQL query
4. To retrieve information from more than one table, you need to join those tables together using JOIN methods. Use the following syntax:
SELECT <list_of_column_names>
FROM <table1>
JOIN <table2> ON <table1.column_name> = <table2.column_name>
WHERE <conditions>;

5. Use the following syntax for Complex Query with Multiple Joins:
SELECT t1.column1, t2.column2, t3.column3, ...
FROM table1 t1
JOIN table2 t2 ON t1.common_column = t2.common_column
JOIN table3 t3 ON t2.common_column = t3.common_column
    -- Add more JOIN statements as needed
WHERE t1.condition_column = condition_value AND t2.another_condition_column = another_condition_value
    -- Add more conditions as needed
GROUP BY t1.group_column, t2.group_column, ...
HAVING aggregate_function(t1.group_column) condition
ORDER BY t1.order_column ASC/DESC, t2.order_column ASC/DESC, ...

6. Use the following syntax for window function query or analytic function query:
SELECT 
    COUNT(*) OVER () AS total_count, 
    *
FROM table_name
WHERE condition_column = 'condition_value';

7. Ensure the SQL syntax is correct and adheres to the database schema and information provided to you.
8. You MUST NOT hallucinate about the tables and their metadata. Use all tables information to build a SQL query.
9. Optimize queries for performance when possible.
10. DO NOT put numerical characters at the very front of SQL variable names.
</rules>

Now to get started, please briefly introduce yourself.
Then provide 3 example questions, DO NOT include table names in example questions presented to user.
"""

def get_table_context(tables, schema):

    context = " "
    properties = {}
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    for table in tables:
        query = f""" DESCRIBE TABLE {DB}.{SCHEMA}.{table}"""
        cursor.execute(query)
        metadata = cursor.fetchall()
        properties[table] = metadata
    context = properties
    return context

def get_system_prompt():
    table_context = get_table_context(
        tables=QUALIFIED_TABLES,
        schema=SCHEMA_PATH
    )
    return GEN_SQL.format(context=table_context)