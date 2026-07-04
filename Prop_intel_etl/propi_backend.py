import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load Environment
load_dotenv()
NEON_CONN_STR = os.getenv("DATABASE_URL")
SILVER_PATH = "C:/Omnijourney_Kofking_github/data/silver/*.parquet"

# Initialize FastAPI
app = FastAPI(title="PropIntel API")

# VERY IMPORTANT: This allows your local HTML files to request data from this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins for local development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_neon_connection():
    return psycopg2.connect(NEON_CONN_STR, cursor_factory=RealDictCursor)

# ==========================================
# ENDPOINT 1: The Pipeline Monitor (Neon DB)
# ==========================================
@app.get("/api/v1/pipeline")
def get_pipeline_state():
    conn = get_neon_connection()
    cur = conn.cursor()
    
    # Get KPIs
    cur.execute("SELECT COUNT(*) as runs FROM pipeline_runs;")
    runs = cur.fetchone()['runs']
    
    cur.execute("SELECT COUNT(*) as count FROM file_lineage WHERE layer = 'BRONZE' AND status = 'SUCCESS';")
    bronze = cur.fetchone()['count']
    
    cur.execute("SELECT COUNT(*) as count FROM file_lineage WHERE layer = 'SILVER' AND status = 'SUCCESS';")
    silver = cur.fetchone()['count']
    
    cur.execute("SELECT COUNT(*) as count FROM file_lineage WHERE status = 'FAILED';")
    failed = cur.fetchone()['count']
    
    # Get Lineage Ledger
    cur.execute("""
        SELECT file_hash, file_name, layer, status, row_count, error_message, updated_at 
        FROM file_lineage 
        ORDER BY updated_at DESC LIMIT 20;
    """)
    lineage = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "kpis": {"runs": runs, "bronze": bronze, "silver": silver, "failed": failed},
        "lineage": [
            {
                "hash": row['file_hash'][:10] + '...', 
                "file": row['file_name'], 
                "layer": row['layer'], 
                "status": row['status'], 
                "rows": row['row_count'], 
                "error": row['error_message']
            } for row in lineage
        ]
    }

# ==========================================
# ENDPOINT 2: Dashboard Analytics (DuckDB)
# ==========================================
@app.get("/api/v1/dashboard")
def get_dashboard_stats():
    con = duckdb.connect()
    
    # Get Market Overview KPIs
    kpi_df = con.execute(f"""
        SELECT 
            COUNT(*) as total_listings,
            AVG(price_per_marla) as avg_marla,
            COUNT(DISTINCT city) as city_count,
            SUM(price) as total_cap
        FROM '{SILVER_PATH}' WHERE is_active = TRUE
    """).df()
    
    # Get City Distribution
    city_df = con.execute(f"""
        SELECT city, COUNT(*) as count 
        FROM '{SILVER_PATH}' 
        WHERE is_active = TRUE AND city IS NOT NULL
        GROUP BY city ORDER BY count DESC LIMIT 4
    """).df()
    
    total = kpi_df['total_listings'][0]
    
    # Format exactly how the UI expects it
    colors = ['bg-blue-500', 'bg-emerald-500', 'bg-amber-500', 'bg-rose-500']
    cities = []
    for i, row in city_df.iterrows():
        pct = round((row['count'] / total) * 100)
        cities.append({"name": row['city'], "pct": pct, "color": colors[i % 4]})

    return {
        "kpis": [
            {"label": "Active Listings", "value": f"{total:,}", "color": "blue"},
            {"label": "Avg Marla Price", "value": f"Rs {kpi_df['avg_marla'][0]/100000:.1f}L", "color": "emerald"},
            {"label": "Cities Tracked", "value": str(kpi_df['city_count'][0]), "color": "amber"},
            {"label": "Market Cap", "value": f"Rs {kpi_df['total_cap'][0]/10000000:.1f}Cr", "color": "rose"}
        ],
        "cities": cities
    }

# ==========================================
# ENDPOINT 3: Market Explorer (DuckDB)
# ==========================================
@app.get("/api/v1/properties")
def search_properties(city: str = "all", prop_type: str = "all"):
    con = duckdb.connect()
    
    query = f"SELECT * FROM '{SILVER_PATH}' WHERE is_active = TRUE "
    
    if city != "all":
        query += f" AND LOWER(city) = '{city.lower()}'"
    if prop_type != "all":
        query += f" AND LOWER(property_type) = '{prop_type.lower()}'"
        
    query += " ORDER BY price DESC LIMIT 50" # Limit to 50 for UI speed
    
    df = con.execute(query).df()
    
    # Format for UI
    properties = []
    for _, row in df.iterrows():
        price_cr = round(row['price'] / 10000000, 2)
        properties.append({
            "id": str(row['property_id']),
            "price": f"{price_cr} Crore",
            "title": f"{row['bedrooms'] if row['bedrooms'] else 'Spacious'} Bed {row['property_type']}",
            "loc": f"{row['location']}, {row['city']}",
            "type": row['property_type'],
            "beds": row['bedrooms'] if row['bedrooms'] else "-",
            "area": f"{row['area_marla']} Marla",
            "lat": row['latitude'],
            "lng": row['longitude']
        })
        
    return properties
