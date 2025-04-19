import pandas as pd
import numpy as np
import mysql.connector
import logging
import matplotlib.pyplot as plt
import seaborn as sns

# Set up logging and plotting style
logging.basicConfig(level=logging.INFO)
sns.set(style="whitegrid")

# Load CSV
csv_path = r"C:/Users/LAMU VAMSI KRISHNA/Downloads/best_sellers_data2.csv"
try:
    df = pd.read_csv(csv_path)
    logging.info("File loaded successfully.")
except FileNotFoundError:
    logging.error("CSV file not found. Please check the path.")
    exit()

# Preprocess
df.columns = df.columns.str.strip().str.lower()
df.drop_duplicates(inplace=True)
df = df[df['product_num_ratings'].notnull()]
df['product_price'] = df['product_price'].replace('[\$,]', '', regex=True)
df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')
df['product_price'].fillna(df['product_price'].median(), inplace=True)
df['product_star_rating'].fillna(df['product_star_rating'].mean(), inplace=True)
df['rankk'] = pd.to_numeric(df['rankk'], errors='coerce').fillna(0).astype(int)
df.reset_index(drop=True, inplace=True)

# Connect to MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='amazon_software'
    )
    cursor = connection.cursor()
    logging.info("Connected to MySQL database.")
except mysql.connector.Error as e:
    logging.error(f"MySQL connection failed: {e}")
    exit()

# Insert data
insert_query = """
    INSERT INTO software_products
    (product_title, product_price, product_star_rating, product_num_ratings, rankk, country)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
data = df[['product_title', 'product_price', 'product_star_rating',
           'product_num_ratings', 'rankk', 'country']].values.tolist()

cursor.executemany(insert_query, data)
connection.commit()
print(f"\n{cursor.rowcount} rows inserted into MySQL successfully.")

# ----------------------
# 📊 INTERACTIVE MENU
# ----------------------
def display_menu():
    print("\n Select an option:")
    print("1. Show Top 10 Best-Selling Software by Review Count")
    print("2. Show Average Rating and Reviews by Country")
    print("3. Show Top 5 Most Reviewed Software Products")
    print("4. Detect Outliers in Price or Rating")
    print("5. Show Average Price and Rating by Product Title (Top 10 by Review Count)")
    print("6. Show Distribution of Software Ratings")
    print("7. Visualize Price vs Number of Reviews (Scatter Plot)")
    print("8. Visualize Top 10 Reviewed Products: Price & Rating")
    print("9. Exit")

def detect_outliers(df, column_name):
    Q1 = df[column_name].quantile(0.25)
    Q3 = df[column_name].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = df[(df[column_name] < lower_bound) | (df[column_name] > upper_bound)]
    return outliers

while True:
    display_menu()
    choice = input("Enter your choice (1-9): ").strip()

    match choice:
        case "1":
            print("\n Top 10 Best-Selling Software by Review Count:")
            cursor.execute("""
                SELECT product_title, product_star_rating, product_num_ratings
                FROM software_products
                ORDER BY product_num_ratings DESC
                LIMIT 10;
            """)
            result = cursor.fetchall()
            df_result = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
            df_result.index += 1
            print(df_result)

        case "2":
            print("\n Average Rating and Reviews by Country:")
            cursor.execute("""
                SELECT 
                    country,
                    ROUND(AVG(product_star_rating), 2) AS avg_rating,
                    ROUND(AVG(product_num_ratings), 2) AS avg_review_count
                FROM software_products
                GROUP BY country
                ORDER BY avg_rating DESC;
            """)
            result = cursor.fetchall()
            df_result = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
            df_result.index += 1
            print(df_result)

        case "3":
            print("\n Top 5 Most Reviewed Software Products:")
            cursor.execute("""
                SELECT product_title, product_num_ratings, country
                FROM software_products
                ORDER BY product_num_ratings DESC
                LIMIT 5;
            """)
            result = cursor.fetchall()
            df_result = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
            df_result.index += 1
            print(df_result)

        case "4":
            print("\n Detecting Outliers in Pricing and Ratings...")
            price_outliers = detect_outliers(df, 'product_price')
            rating_outliers = detect_outliers(df, 'product_star_rating')

            print("\n Price Outliers (Top 5):")
            print(price_outliers[['product_title', 'product_price', 'country']].head(5))

            print("\n Rating Outliers (Top 5):")
            print(rating_outliers[['product_title', 'product_star_rating', 'country']].head(5))

            print(f"\n Found {len(price_outliers)} price outliers and {len(rating_outliers)} rating outliers.")


        case "5":
            print("\n Average Price and Rating by Product Title (Top 10 by Review Count):")
            cursor.execute("""
                SELECT 
                    product_title,
                    ROUND(AVG(product_price), 2) AS avg_price,
                    ROUND(AVG(product_star_rating), 2) AS avg_rating,
                    SUM(product_num_ratings) AS total_reviews
                FROM software_products
                GROUP BY product_title
                ORDER BY total_reviews DESC
                LIMIT 10;
            """)
            result = cursor.fetchall()
            df_result = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
            df_result.index += 1
            print(df_result)

        case "6":
            print("\n Distribution of Software Ratings:")
            plt.figure(figsize=(8, 5))
            sns.histplot(df['product_star_rating'], bins=10, kde=True, color='skyblue')
            plt.title("Distribution of Software Ratings")
            plt.xlabel("Star Rating")
            plt.ylabel("Number of Products")
            plt.tight_layout()
            plt.show()

        case "7":
            print("\n Price vs Number of Reviews (Scatter Plot):")
            plt.figure(figsize=(8, 5))
            sns.scatterplot(x='product_price', y='product_num_ratings', hue='product_star_rating', data=df, palette='coolwarm')
            plt.title("Price vs Number of Reviews")
            plt.xlabel("Price ($)")
            plt.ylabel("Number of Reviews")
            plt.tight_layout()
            plt.show()

        case "8":
            print("\n Top 10 Reviewed Products with Ratings and Prices (Bar Chart):")
            top10 = df.sort_values(by='product_num_ratings', ascending=False).head(10)
            fig, ax1 = plt.subplots(figsize=(10, 6))

            sns.barplot(x='product_title', y='product_price', data=top10, ax=ax1, color='lightblue', label='Price')
            ax1.set_ylabel('Price ($)')
            ax1.set_xticklabels(top10['product_title'], rotation=45, ha='right')

            ax2 = ax1.twinx()
            sns.lineplot(x='product_title', y='product_star_rating', data=top10, ax=ax2, color='red', marker='o', label='Rating')
            ax2.set_ylabel('Star Rating')

            plt.title("Top 10 Reviewed Products: Price vs Rating")
            fig.tight_layout()
            plt.show()

        
        case "9":
            print("\n Exiting. Goodbye!")
            break

        case _:
            print(" Invalid choice.")

# Cleanup
cursor.close()
connection.close()
print(" MySQL connection closed.")

