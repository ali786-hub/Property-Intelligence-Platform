"""
Exploratory Data Analysis on the Silver Layer.
Purpose: Understand the data's dimensions, distributions, and potential
before designing the Gold Layer architecture.
"""
import duckdb

con = duckdb.connect()
s = 'C:/Omnijourney_Kofking_github/data/silver/*.parquet'

# 1. Total row count
print('=== 1. TOTAL ROWS ===')
print(con.execute(f"SELECT COUNT(*) as total_rows FROM read_parquet('{s}')").df())

# 2. Date range of the dataset
print('\n=== 2. DATE RANGE ===')
print(con.execute(f"SELECT MIN(TRY_CAST(date_added AS DATE)) as oldest, MAX(TRY_CAST(date_added AS DATE)) as newest FROM read_parquet('{s}')").df())

# 3. Top 10 cities by listing count
print('\n=== 3. TOP 10 CITIES ===')
print(con.execute(f"SELECT city, COUNT(*) as listings FROM read_parquet('{s}') GROUP BY city ORDER BY listings DESC LIMIT 10").df())

# 4. Property types breakdown
print('\n=== 4. PROPERTY TYPES ===')
print(con.execute(f"SELECT property_type, COUNT(*) as listings FROM read_parquet('{s}') GROUP BY property_type ORDER BY listings DESC").df())

# 5. Purpose (For Sale vs For Rent)
print('\n=== 5. PURPOSE (Sale vs Rent) ===')
print(con.execute(f"SELECT purpose, COUNT(*) as listings FROM read_parquet('{s}') GROUP BY purpose ORDER BY listings DESC").df())

# 6. Price distribution per purpose
print('\n=== 6. PRICE STATS BY PURPOSE ===')
print(con.execute(f"""
    SELECT purpose,
           ROUND(AVG(price),0) as avg_price,
           ROUND(MEDIAN(price),0) as median_price,
           MIN(price) as min_price,
           MAX(price) as max_price
    FROM read_parquet('{s}')
    WHERE price > 0
    GROUP BY purpose
""").df())

# 7. Average price per marla by top 5 cities
print('\n=== 7. AVG PRICE PER MARLA (Top 5 Cities) ===')
print(con.execute(f"""
    SELECT city,
           COUNT(*) as listings,
           ROUND(AVG(price_per_marla),0) as avg_price_per_marla,
           ROUND(MEDIAN(price_per_marla),0) as median_price_per_marla
    FROM read_parquet('{s}')
    WHERE price_per_marla > 0 AND price_per_marla IS NOT NULL
    GROUP BY city
    ORDER BY listings DESC
    LIMIT 5
""").df())

# 8. Province breakdown
print('\n=== 8. PROVINCES ===')
print(con.execute(f"SELECT province_name, COUNT(*) as listings FROM read_parquet('{s}') GROUP BY province_name ORDER BY listings DESC").df())

# 9. Cardinality of key dimensions
print('\n=== 9. DIMENSION CARDINALITY ===')
print(con.execute(f"""
    SELECT
        COUNT(DISTINCT city) as unique_cities,
        COUNT(DISTINCT location) as unique_locations,
        COUNT(DISTINCT location_id) as unique_location_ids,
        COUNT(DISTINCT agency) as unique_agencies,
        COUNT(DISTINCT agent) as unique_agents,
        COUNT(DISTINCT property_type) as unique_property_types,
        COUNT(DISTINCT province_name) as unique_provinces
    FROM read_parquet('{s}')
""").df())

# 10. How many duplicate property_ids exist (same property listed multiple times)?
print('\n=== 10. DUPLICATE PROPERTY IDS (SCD Type 2 candidates) ===')
print(con.execute(f"""
    SELECT duplicate_count, COUNT(*) as how_many_properties
    FROM (
        SELECT property_id, COUNT(*) as duplicate_count
        FROM read_parquet('{s}')
        GROUP BY property_id
    )
    GROUP BY duplicate_count
    ORDER BY duplicate_count ASC
    LIMIT 10
""").df())

# 11. Listings with GPS coordinates vs without
print('\n=== 11. GPS COVERAGE ===')
print(con.execute(f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as has_gps,
        SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) as missing_gps
    FROM read_parquet('{s}')
""").df())

# 12. Top 10 agencies by volume
print('\n=== 12. TOP 10 AGENCIES ===')
print(con.execute(f"""
    SELECT agency, COUNT(*) as listings
    FROM read_parquet('{s}')
    GROUP BY agency
    ORDER BY listings DESC
    LIMIT 10
""").df())

# 13. Bedrooms distribution
print('\n=== 13. BEDROOMS DISTRIBUTION ===')
print(con.execute(f"""
    SELECT bedrooms, COUNT(*) as listings
    FROM read_parquet('{s}')
    GROUP BY bedrooms
    ORDER BY bedrooms ASC
    LIMIT 15
""").df())

# 14. Monthly listing volume (how many properties added per month)
print('\n=== 14. MONTHLY LISTING VOLUME ===')
print(con.execute(f"""
    SELECT
        DATE_TRUNC('month', TRY_CAST(date_added AS DATE)) as month,
        COUNT(*) as listings
    FROM read_parquet('{s}')
    WHERE TRY_CAST(date_added AS DATE) IS NOT NULL
    GROUP BY month
    ORDER BY month ASC
""").df())

con.close()
