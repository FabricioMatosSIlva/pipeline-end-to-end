import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from pathlib import Path

# ========================
# Configurações
# ========================
TRUSTED_PATH = Path("data/trusted")

# Usa variáveis de ambiente com fallback para valores padrão (execução local)
DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "de_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "de_password"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "fakestore_dw")
}

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    filename="logs/load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========================
# Funções de carga
# ========================
def load_dim_products():
    df = pd.read_parquet(TRUSTED_PATH / "products.parquet")

    df_dim = df[["product_id", "title", "category_name", "price_usd"]]

    df_dim.to_sql(
        "dim_products",
        engine,
        if_exists="append",
        index=False
    )

    logging.info("Loaded dim_products")


def load_dim_users():
    df = pd.read_parquet(TRUSTED_PATH / "users.parquet")

    # Verificar colunas disponíveis (defensivo)
    required_columns = [
        "user_id",
        "email",
        "username",
        "address.city"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column in users data: {col}")

    df_dim = df[[
        "user_id",
        "email",
        "username",
        "address.city"
    ]].rename(columns={
        "address.city": "city"
    })

    df_dim.to_sql(
        "dim_users",
        engine,
        if_exists="append",
        index=False
    )

    logging.info("Loaded dim_users")


def load_fact_carts():
    df = pd.read_parquet(TRUSTED_PATH / "carts.parquet")

    df["total_items"] = df["products"].apply(len)

    df_fact = df[[
        "cart_id",
        "user_id",
        "cart_date",
        "total_items"
    ]]

    df_fact.to_sql(
        "fact_carts",
        engine,
        if_exists="append",
        index=False
    )

    logging.info("Loaded fact_carts")


def main():
    logging.info("Starting load process")

    load_dim_products()
    load_dim_users()
    load_fact_carts()

    logging.info("Load process finished successfully")


if __name__ == "__main__":
    main()
