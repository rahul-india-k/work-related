import base64
import pandas as pd
from collections import defaultdict

# Read delta table into Spark DataFrame
df = spark.read.format("delta").load("/path/to/delta/table")

# Collect rows into driver (⚠️ works if data fits in memory, 
# otherwise you'd parallelize)
rows = df.select("LogSchema", "LogSchemaTypes", "LogFile").collect()

# Dictionary to accumulate per schema
schema_to_data = defaultdict(list)

for row in rows:
    schema = row["LogSchema"].replace(" ", "")  # clean spaces
    schema_cols = schema.split(",")

    # Decode base64
    decoded_str = base64.b64decode(row["LogFile"]).decode("utf-8")

    # Each row in decoded string is newline-separated
    lines = decoded_str.strip().split("\n")

    # Skip header line if it's the same as schema
    if lines[0].replace(" ", "") == schema:
        lines = lines[1:]

    # Parse each row
    for line in lines:
        values = [v.strip() for v in line.split(",")]
        schema_to_data[schema].append(values)

# Convert each schema's collected rows into a pandas DataFrame
schema_to_df = {}
for schema, rows in schema_to_data.items():
    cols = schema.split(",")
    schema_to_df[schema] = pd.DataFrame(rows, columns=cols)

# Example: Access the DataFrame for a specific schema
for schema, pdf in schema_to_df.items():
    print(f"Schema: {schema}")
    print(pdf.head())
