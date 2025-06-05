import pandas as pd
from typing import Optional

def extract_local(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load a parquet file from a local path into a pandas DataFrame.

    Args:
        file_path (str): Path to the local parquet file.

    Returns:
        Optional[pandas.DataFrame]: DataFrame loaded from parquet file or None if error.
    """
    try:
        print(f"Loading {file_path} from local path...")
        df = pd.read_parquet(file_path, engine="pyarrow")  # or engine='fastparquet'
        return df
    except ImportError as e:
        print(f"Error loading {file_path}: {e}. Please install pyarrow or fastparquet.")
    except Exception as e:
        print(f"Unexpected error loading {file_path}: {e}")
    return None
