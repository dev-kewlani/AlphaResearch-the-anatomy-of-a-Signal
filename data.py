import pandas as pd

# Read the parquet file
df = pd.read_parquet('feature.parq')

# Display the data
print(df.head())
print(df.info())

print("I am Kate")
print("I Love Stats!!!")