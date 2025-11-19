from orchestration_prompt_engineering import route

print(route("How many fatalities were there in 2022?"))        # expected: "sql"
print(route("Explain what the WEATHER variable means"))        # expected: "rag"
print(route("Give me statistics and explain them"))            # expected: "both"