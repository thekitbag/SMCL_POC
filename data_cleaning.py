# data_cleaning.py
import pandas as pd

def clean_data(df):
    """
    Cleans the uploaded data.

    Args:
        df (pd.DataFrame): The DataFrame containing the raw data.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    # 1. Remove Duplicate Rows (based on email, for example)
    df.drop_duplicates(subset=['email'], keep='first', inplace=True)  # Adjust 'email' if your duplicate key is different

    # 2. Handle Missing Values (example: fill missing emails with a placeholder)
    df['email'].fillna('noemail@example.com', inplace=True)

    # 3. Data Validation (example: check if email format is valid using regex)
    # ... (Add your email validation logic here) ...

    # 4. Other Cleaning as needed...
    # ... (Trim whitespace, convert data types, etc.) ...

    return df