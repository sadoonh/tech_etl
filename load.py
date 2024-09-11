import pandas as pd
from sqlalchemy import create_engine

# Define the database connection string
DATABASE_URI = 'postgresql://username:password@localhost:5432/your_database'

# Create a SQLAlchemy engine for connecting to the database
engine = create_engine(DATABASE_URI)

def load_db_to_df(df: pd.DataFrame, table_name: str) -> None:
    """
    Loads a DataFrame into the specified table in the database.

    Args:
        df (pd.DataFrame): The DataFrame to be loaded.
        table_name (str): The name of the table where the data should be loaded.

    Returns:
        None
    """
    # Check if DataFrame is not empty
    if df.empty:
        return None

    # Write DataFrame to the database using pandas' to_sql method
    df.to_sql(table_name, engine, if_exists='replace', index=False)
