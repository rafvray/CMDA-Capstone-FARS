from metadata_loader import load_fars_codebook
import pandas as pd
from sql_query_chain import get_column_metadata_context

# Load your codebook
metadata = load_fars_codebook("../../fars_codebook.csv")

# Pick the table and column to test
table = "accident"
column = "WEATHER"

if table in metadata and column in metadata[table]:
    codes = metadata[table][column]["codes"]
    print(f"WEATHER mapping ({len(codes)} codes found):")
    for code, label in codes.items():
        print(f"{code} -> {label}")
else:
    print(f"{table}.{column} not found in metadata.")

df = pd.DataFrame({
    "WEATHER": [3, 4, 98, 2, 5, 8],
    "FATALS": [1, 4, 891, 103, 6, 1]
})

context = get_column_metadata_context(df, sql_query="")
print(context)

