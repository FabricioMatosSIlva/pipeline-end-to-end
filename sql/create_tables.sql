CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    title TEXT,
    category_name TEXT,
    price_usd NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS dim_users (
    user_id INT PRIMARY KEY,
    email TEXT,
    username TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS fact_carts (
    cart_id INT PRIMARY KEY,
    user_id INT,
    cart_date DATE,
    total_items INT,
    FOREIGN KEY (user_id) REFERENCES dim_users(user_id)
);