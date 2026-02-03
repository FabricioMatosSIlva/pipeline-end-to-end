"""
Transform Layer - Data Pipeline
Processes RAW JSON files into TRUSTED Parquet format.

This module handles:
- Loading the most recent RAW JSON file per endpoint
- Transforming and validating data according to Fake Store API schema
- Writing validated data to TRUSTED layer as Parquet files
"""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

# =============================================================================
# Configuration
# =============================================================================
RAW_PATH = Path("data/raw")
TRUSTED_PATH = Path("data/trusted")

# Ensure directories exist
TRUSTED_PATH.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# Schema Definitions (based on Fake Store API)
# =============================================================================
PRODUCTS_REQUIRED_COLUMNS = {"id", "title", "price", "category"}
USERS_REQUIRED_COLUMNS = {"id", "email", "username"}
CARTS_REQUIRED_COLUMNS = {"id", "userId", "date", "products"}


# =============================================================================
# Utility Functions
# =============================================================================
class TransformationError(Exception):
    """Custom exception for transformation errors."""
    pass


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: set[str],
    entity_name: str
) -> None:
    
    actual_columns = set(df.columns)
    missing_columns = required_columns - actual_columns

    if missing_columns:
        raise TransformationError(
            f"Missing required columns in {entity_name}: {missing_columns}. "
            f"Available columns: {actual_columns}"
        )


def load_json_files(endpoint: str) -> list[dict[str, Any]]:

    pattern = f"{endpoint}_*.json"
    files = sorted(RAW_PATH.glob(pattern), reverse=True)

    if not files:
        raise FileNotFoundError(
            f"No RAW files found for endpoint '{endpoint}' "
            f"with pattern '{pattern}' in {RAW_PATH.absolute()}"
        )

    latest_file = files[0]
    logger.info(f"Loading {endpoint} from: {latest_file.name}")

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise TransformationError(
            f"Expected list of records for {endpoint}, got {type(data).__name__}"
        )

    if len(data) == 0:
        raise TransformationError(
            f"RAW file for {endpoint} is empty: {latest_file.name}"
        )

    logger.info(f"Loaded {len(data)} records for {endpoint}")
    return data


# =============================================================================
# Transformation Functions
# =============================================================================
def transform_products(data: list[dict[str, Any]]) -> pd.DataFrame:

    if not data:
        raise TransformationError("No product data provided for transformation")

    df = pd.json_normalize(data)
    logger.info(f"Products - columns after normalization: {list(df.columns)}")

    # Validate required columns exist
    validate_dataframe(df, PRODUCTS_REQUIRED_COLUMNS, "products")

    # Rename columns for dimensional model
    df = df.rename(columns={
        "id": "product_id",
        "price": "price_usd",
        "category": "category_name"
    })

    # Ensure correct data types
    df["product_id"] = df["product_id"].astype(int)
    df["price_usd"] = df["price_usd"].astype(float)

    logger.info(f"Products - transformed {len(df)} records")
    return df


def transform_users(data: list[dict[str, Any]]) -> pd.DataFrame:

    if not data:
        raise TransformationError("No user data provided for transformation")

    df = pd.json_normalize(data)
    logger.info(f"Users - columns after normalization: {list(df.columns)}")

    # Validate required columns exist (using original column names before rename)
    validate_dataframe(df, USERS_REQUIRED_COLUMNS, "users")

    # Rename columns for dimensional model
    df = df.rename(columns={
        "id": "user_id"
    })

    # Ensure correct data types
    df["user_id"] = df["user_id"].astype(int)

    # Validate nested address fields exist (created by json_normalize)
    address_columns = {"address.city", "address.street", "address.zipcode"}
    available_columns = set(df.columns)
    missing_address = address_columns - available_columns

    if missing_address:
        logger.warning(
            f"Users - missing expected address columns: {missing_address}. "
            f"This may indicate a schema change in the Fake Store API."
        )

    logger.info(f"Users - transformed {len(df)} records")
    return df


def transform_carts(data: list[dict[str, Any]]) -> pd.DataFrame:

    if not data:
        raise TransformationError("No cart data provided for transformation")

    df = pd.json_normalize(data)
    logger.info(f"Carts - columns after normalization: {list(df.columns)}")

    # Validate required columns exist
    validate_dataframe(df, CARTS_REQUIRED_COLUMNS, "carts")

    # Validate products column contains valid arrays
    for idx, products in enumerate(df["products"]):
        if not isinstance(products, list):
            raise TransformationError(
                f"Cart at index {idx} has invalid products type: "
                f"expected list, got {type(products).__name__}"
            )

    # Rename columns for dimensional model
    df = df.rename(columns={
        "id": "cart_id",
        "userId": "user_id",
        "date": "cart_date"
    })

    # Ensure correct data types
    df["cart_id"] = df["cart_id"].astype(int)
    df["user_id"] = df["user_id"].astype(int)
    df["cart_date"] = pd.to_datetime(df["cart_date"])

    logger.info(f"Carts - transformed {len(df)} records")
    return df


# =============================================================================
# Main Execution
# =============================================================================
def main() -> None:
    """
    Execute the transformation pipeline.

    Loads RAW data for all endpoints, transforms it, and writes
    to TRUSTED layer as Parquet files.
    """
    logger.info("=" * 60)
    logger.info("Starting transformation process")
    logger.info("=" * 60)

    try:
        # Load RAW data (most recent file per endpoint)
        products_data = load_json_files("products")
        users_data = load_json_files("users")
        carts_data = load_json_files("carts")

        # Transform data
        df_products = transform_products(products_data)
        df_users = transform_users(users_data)
        df_carts = transform_carts(carts_data)

        # Write to TRUSTED layer (Parquet format)
        df_products.to_parquet(TRUSTED_PATH / "products.parquet", index=False)
        logger.info(f"Wrote {len(df_products)} products to TRUSTED layer")

        df_users.to_parquet(TRUSTED_PATH / "users.parquet", index=False)
        logger.info(f"Wrote {len(df_users)} users to TRUSTED layer")

        df_carts.to_parquet(TRUSTED_PATH / "carts.parquet", index=False)
        logger.info(f"Wrote {len(df_carts)} carts to TRUSTED layer")

        logger.info("=" * 60)
        logger.info("Transformation completed successfully")
        logger.info("=" * 60)

    except (FileNotFoundError, TransformationError) as e:
        logger.error(f"Transformation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {e}")
        raise


if __name__ == "__main__":
    main()
