# test_sql_query_chain.py
# Test Ollama SQL generation and Databricks execution separately

from sql_query_chain import ask_fars_database

# ---------------- Example Usage with Ollama SQL generation ----------------
questions = [
    "How many total fatalities were there in 2023?",
    "Show me the weather (WEATHER) and number of fatalities (FATALS) for accidents in Virginia (STATE=51) in 2022",
    "How many accidents involved a 17-year-old driver?"
]

for i, q in enumerate(questions, start=1):
    print(f"\nQ{i}: {q}")
    # This will generate SQL via Ollama and attempt to run it
    result = ask_fars_database(q)
    print(f"A{i}: {result}")