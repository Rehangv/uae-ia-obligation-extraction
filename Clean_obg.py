import pandas as pd
import re
def split_obligations(text):
    if pd.isna(text):
        return []

    text = text.strip()

    # Pattern: split at Roman numeral points (i., ii., iii., iv., ...)
    parts = re.split(r'(?m)^\s*(?=[ivxlcdm]+\.)', text, flags=re.IGNORECASE)

    # Remove empty strings
    parts = [p.strip() for p in parts if p.strip()]

    return parts

df=pd.read_excel(r"C:\Users\QQ417YB\Compliance\Final_Code_Path\Input Files - Obligations\MAS Notice 307\MAS Notice 307 obligations.xlsx")
print(df.columns)
expanded_rows = []

for idx, row in df.iterrows():
    obligations_text = row["Obligations Breakdown"]
    split_items = split_obligations(obligations_text)

    for item in split_items:
        new_row = row.copy()
        new_row["Obligations_Split"] = item
        expanded_rows.append(new_row)
final_df=pd.DataFrame(expanded_rows)
final_df.to_excel(r"C:\Users\QQ417YB\Compliance\Final_Code_Path\Input Files - Obligations\MAS Notice 307\MAS Notice 307 obligations corrected.xlsx")