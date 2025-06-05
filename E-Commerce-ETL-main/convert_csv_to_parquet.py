import os
import pandas as pd

# ✅ Update these paths to your actual CSV and output folders
input_folder = "Dataset/Raw-Dataset/E-Commerce"
output_folder = "Dataset/Data"

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Loop through each CSV file in the input folder
for file in os.listdir(input_folder):
    if file.endswith(".csv"):
        csv_path = os.path.join(input_folder, file)
        df = pd.read_csv(csv_path)

        parquet_filename = file.replace(".csv", ".parquet")
        parquet_path = os.path.join(output_folder, parquet_filename)

        print(f"Converting {file} -> {parquet_filename}")
        df.to_parquet(parquet_path, index=False)

print("✅ All CSV files have been converted to Parquet format.")
