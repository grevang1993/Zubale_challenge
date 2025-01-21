import pandas as pd
import numpy as np

def dynamic_type_conversion(df):
    """Dynamically converts DataFrame columns to appropriate data types.

    Args:
        df: The input DataFrame.

    Returns:
        A new DataFrame with converted data types, or the original DataFrame 
        if no conversions were possible. Returns None if the input is not a DataFrame.
        Prints warnings for problematic conversions.
    """

    if not isinstance(df, pd.DataFrame):
        print("Error: Input must be a pandas DataFrame.")
        return None

    df_converted = df.copy()  # Avoid modifying the original DataFrame

    for col in df_converted.columns:
        original_type = df_converted[col].dtype

        if pd.api.types.is_object_dtype(df_converted[col]):  # Check for 'object' type (strings/mixed)
            try:
                # 1. Try numeric conversion (int/float)
                df_converted[col] = pd.to_numeric(df_converted[col], errors='raise') # 'raise' will stop execution if the conversion fails
                if df_converted[col].astype(int) == df_converted[col]:
                    df_converted[col] = df_converted[col].astype(int) #convert to int if possible
                print(f"Column '{col}' converted from object to numeric.")
                continue # Skip to the next column
            except ValueError:
                pass # If numeric conversion fails, try other conversions

            try:
                # 2. Try datetime conversion
                df_converted[col] = pd.to_datetime(df_converted[col], errors='raise')
                print(f"Column '{col}' converted from object to datetime.")
                continue
            except (ValueError, TypeError): #handle cases where it can be none
                pass

            try:
                # 3. Try boolean conversion (case-insensitive)
                bool_map = {'true': True, 'false': False, 'TRUE': True, 'FALSE': False, 'True': True, 'False': False}
                if all(x in bool_map for x in df_converted[col].unique()):
                    df_converted[col] = df_converted[col].map(bool_map)
                    print(f"Column '{col}' converted from object to boolean.")
                    continue
            except AttributeError:
                pass #in case there are nan values

        elif pd.api.types.is_integer_dtype(df_converted[col]):
            if (df_converted[col].min() >= 0) and (df_converted[col].max() <= 255):
              df_converted[col] = df_converted[col].astype(np.uint8)
              print(f"Column '{col}' converted from int to uint8.")
        elif pd.api.types.is_float_dtype(df_converted[col]):
            if (df_converted[col].min() >= 0) and (df_converted[col].max() <= 1):
                df_converted[col] = df_converted[col].astype(np.float16)
                print(f"Column '{col}' converted from float to float16.")

    return df_converted

