import json
import os
import glob
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "123456",

}

JSON_DIR = "tiki_products/products-200001-400000"
FILE_PATTERN = "products_*.json"

def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                url_key TEXT UNIQUE,
                price NUMERIC(15,2),
                description TEXT
            );
            CREATE TABLE IF NOT EXISTS product_images (
                id SERIAL PRIMARY KEY,
                product_id BIGINT REFERENCES products(id) ON DELETE CASCADE,
                image_url TEXT NOT NULL,
                position INT NOT NULL DEFAULT 0
            );
            
            CREATE INDEX IF NOT EXISTS idx_product_images_product_id ON product_images(product_id);
                       
        """)
        conn.commit()
        print("Tables created successfully or tables is already exists.")

def load_json_files(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_data(conn, data):
    products_rows = []
    image_rows = []

    for p in data:
        products_rows.append((p["id"], p.get("name", ""), p.get("url_key", ""), p.get("price", None), p.get("description", "")))
        for pos, url in enumerate(p.get("images_url", [])):
            image_rows.append((p["id"], url, pos))

    with conn.cursor() as cursor:
        execute_values(cursor, """
            INSERT INTO products (id, name, url_key, price, description)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
                       
        """, products_rows)

        if image_rows:
            execute_values(cursor, """
                INSERT INTO product_images (product_id, image_url, position)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, image_rows)

        conn.commit()
        return len(products_rows), len(image_rows)


def main():
    pattern = os.path.join(JSON_DIR, FILE_PATTERN)
    files = sorted(glob.glob(pattern))


    if not files:
        print("No JSON files found.")
        return
    
    print(f"Found {len(files)} JSON files. Starting to process...")

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        create_table(conn)

        total_products = 0
        total_images = 0

        for filepath in files:
            filename = os.path.basename(filepath)
            try:
                products = load_json_files(filepath)
                n_prod, n_img = insert_data(conn, products)
                total_products += n_prod
                total_images += n_img
                print(f"  ✔ {filename}: {n_prod} sản phẩm, {n_img} ảnh")
            except json.JSONDecodeError as e:
                print(f"  ✖ {filename}: JSON decode error - {e}")
                conn.rollback()
            except Exception as e:
                print(f"  ✖ {filename}: Error - {e}")
                conn.rollback()
        print(f"Finished processing. Total products: {total_products}, Total images: {total_images}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()    
