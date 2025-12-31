import re
 
# Central routing definition
FILE_TABLE_MAPPING = {
    "ORDERS": ["ORDER", "ORDERS"],
    "CUSTOMER": ["CUSTOMER", "CUST"],
    "PART": ["PART"],
    "REGION": ["REGION"],
    "SUPPLIER": ["SUPPLIER", "SUPP"]
}
 
 
def resolve_table_from_filename(filename):
    """
    Determines target Snowflake table from filename.
    Case-insensitive.
    Raises error if no or multiple matches.
    """
 
    filename_upper = filename.upper()
    matched_tables = []
 
    for table, keywords in FILE_TABLE_MAPPING.items():
        for keyword in keywords:
            if keyword in filename_upper:
                matched_tables.append(table)
                break
 
    if len(matched_tables) == 0:
        raise ValueError(f"No table match found for file: {filename}")
 
    if len(matched_tables) > 1:
        raise ValueError(
            f"Ambiguous file routing for {filename}: {matched_tables}"
        )
 
    return matched_tables[0]