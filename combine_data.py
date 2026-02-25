import pandas as pd
import os
import glob

DATA_DIR = "/Users/finnjohnson/Documents/Boldt Projects/ACC To Live Drivelog QC/ACC Data"
OUTPUT_PATH = "/Users/finnjohnson/Documents/Boldt Projects/ACC To Live Drivelog QC/ACC_Combined.csv"

STANDARD_COLS = ["UPN", "N_Y", "E_X", "Elv_Z", "Source", "Date_Scanned"]
RENAME_MAP = {"Northing": "N_Y", "Easting": "E_X", "Elevation": "Elv_Z"}
HEADERLESS_5 = ["UPN", "N_Y", "E_X", "Elv_Z", "Source"]
HEADERLESS_4 = ["UPN", "N_Y", "E_X", "Elv_Z"]

frames = []

# Read all CSVs
for filepath in sorted(glob.glob(os.path.join(DATA_DIR, "*.csv"))):
    fname = os.path.basename(filepath)
    # Peek at first row to detect if there's a header
    peek = pd.read_csv(filepath, nrows=0, header=None)
    first_val = str(peek.columns[0]) if len(peek.columns) > 0 else ""

    # Try reading with header
    test = pd.read_csv(filepath, nrows=1)
    has_header = any(col in test.columns for col in ["UPN", "Northing", "Source"])

    if has_header:
        df = pd.read_csv(filepath)
        df.rename(columns=RENAME_MAP, inplace=True)
    else:
        df = pd.read_csv(filepath, header=None)
        if df.shape[1] == 5:
            df.columns = HEADERLESS_5
        elif df.shape[1] == 4:
            df.columns = HEADERLESS_4
        else:
            print(f"WARNING: {fname} has {df.shape[1]} columns, skipping")
            continue

    df["source_file"] = fname
    frames.append(df)
    print(f"  {fname}: {len(df)} rows, cols={list(df.columns)}")

# Read xlsx
xlsx_path = os.path.join(DATA_DIR, "BS_INV-31_S01.xlsx")
if os.path.exists(xlsx_path):
    df = pd.read_excel(xlsx_path)
    df.rename(columns=RENAME_MAP, inplace=True)
    df["source_file"] = "BS_INV-31_S01.xlsx"
    frames.append(df)
    print(f"  BS_INV-31_S01.xlsx: {len(df)} rows, cols={list(df.columns)}")

# Combine
combined = pd.concat(frames, ignore_index=True)
print(f"\nTotal rows before filter: {len(combined)}")

# Show unique Source values for visibility
print(f"\nUnique Source values ({combined['Source'].nunique()}):")
for s in sorted(combined["Source"].dropna().unique()):
    has_b = " <-- contains _B" if "_B" in str(s) else ""
    print(f"  {s}{has_b}")

# Filter out rows where Source contains '_B'
mask = combined["Source"].str.contains("_B", na=False)
removed = mask.sum()
combined = combined[~mask]
print(f"\nRemoved {removed} rows with '_B' in Source")
print(f"Total rows after filter: {len(combined)}")

# Save
combined.to_csv(OUTPUT_PATH, index=False)
print(f"\nSaved to: {OUTPUT_PATH}")
