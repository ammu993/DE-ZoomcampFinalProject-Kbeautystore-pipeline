"""
K-Beauty Raw Data Generator
============================
Simulates a real e-commerce store API export.
Writes raw nested JSON to GCS, partitioned by date.

GCS layout produced:
  raw/orders/date=YYYY-MM-DD/orders.json     <- one file per day
  raw/products/snapshot_date=YYYY-MM-DD/products.json  <- daily product snapshot

Run modes:
  python generate_raw.py seed    # bootstraps 1 year of history to GCS
  python generate_raw.py daily   # writes only yesterday (called by Kestra daily)
  python generate_raw.py local   # writes to ./local_output/ instead of GCS (dev/test)

Environment variables (set in .env or Kestra):
  GCS_BUCKET   e.g. kbeauty-data-lake-end2end
  GCS_PROJECT  e.g. end2end-pipeline-kbeautystore
"""

import os
import json
import random
import argparse
from datetime import datetime, timedelta, date

import numpy as np
from faker import Faker

# ── optional GCS import (skipped in local mode) ──
try:
    from google.cloud import storage as gcs
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

fake = Faker("de_DE")          # German locale for realistic EU names/cities
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# ──────────────────────────────────────────
# CONFIG  (edit or override via env vars)
# ──────────────────────────────────────────
GCS_BUCKET  = os.getenv("GCS_BUCKET",  "de-zoomcamp-ammu-spark")
GCS_PROJECT = os.getenv("GCS_PROJECT", "lyrical-compass-485510-f8")
LOCAL_DIR   = "local_output"

DAILY_ORDERS_MEAN = 80   # ~29k orders/year

# ──────────────────────────────────────────
# REFERENCE DATA
# ──────────────────────────────────────────

PRODUCTS = [
    # (product_id, name, brand, category, subcategory, unit_price, cost_price, sku)
    ("P0001","COSRX Snail Serum 96","COSRX","Face","Serum",24.99,9.50,"SKU-COS-00001"),
    ("P0002","COSRX Low pH Cleanser","COSRX","Face","Cleanser",14.99,5.20,"SKU-COS-00002"),
    ("P0003","COSRX AHA/BHA Toner","COSRX","Face","Toner",19.99,7.10,"SKU-COS-00003"),
    ("P0004","Laneige Water Sleeping Mask","Laneige","Face","Moisturiser",29.99,11.00,"SKU-LAN-00004"),
    ("P0005","Laneige Lip Sleeping Mask","Laneige","Lips","Lip Mask",22.99,8.40,"SKU-LAN-00005"),
    ("P0006","Laneige Water Bank Serum","Laneige","Face","Serum",42.99,15.50,"SKU-LAN-00006"),
    ("P0007","Beauty of Joseon Glow Serum","Beauty of Joseon","Face","Serum",18.99,6.80,"SKU-BOJ-00007"),
    ("P0008","Beauty of Joseon SPF 50","Beauty of Joseon","Face","SPF",16.99,5.90,"SKU-BOJ-00008"),
    ("P0009","Beauty of Joseon Cleansing Balm","Beauty of Joseon","Face","Cleanser",19.99,7.20,"SKU-BOJ-00009"),
    ("P0010","Innisfree Green Tea Cream","Innisfree","Face","Moisturiser",22.99,8.10,"SKU-INN-00010"),
    ("P0011","Innisfree Jeju Volcanic Pore Scrub","Innisfree","Face","Cleanser",12.99,4.50,"SKU-INN-00011"),
    ("P0012","Some By Mi AHA-BHA-PHA Toner","Some By Mi","Face","Toner",17.99,6.20,"SKU-SBM-00012"),
    ("P0013","Some By Mi Snail Truecica Cream","Some By Mi","Face","Moisturiser",21.99,7.80,"SKU-SBM-00013"),
    ("P0014","Klairs Supple Prep Toner","Klairs","Face","Toner",24.99,9.00,"SKU-KLA-00014"),
    ("P0015","Klairs Rich Moist Soothing Cream","Klairs","Face","Moisturiser",27.99,10.20,"SKU-KLA-00015"),
    ("P0016","Isntree Hyaluronic Acid Toner","Isntree","Face","Toner",19.99,7.10,"SKU-ISN-00016"),
    ("P0017","Torriden Dive-in Serum","Torriden","Face","Serum",22.99,8.20,"SKU-TOR-00017"),
    ("P0018","Anua Heartleaf Pore Toner","Anua","Face","Toner",21.99,7.60,"SKU-ANU-00018"),
    ("P0019","Anua Heartleaf Soothing Cream","Anua","Face","Moisturiser",24.99,8.90,"SKU-ANU-00019"),
    ("P0020","Dr.Jart+ Cicapair Cream","Dr.Jart+","Face","Moisturiser",44.99,16.50,"SKU-DRJ-00020"),
    ("P0021","Dr.Jart+ Ceramidin Cream","Dr.Jart+","Face","Moisturiser",38.99,14.20,"SKU-DRJ-00021"),
    ("P0022","Skin1004 Madagascar Centella Toner","Skin1004","Face","Toner",16.99,5.80,"SKU-SKN-00022"),
    ("P0023","Skin1004 Hyalu-Cica Water Cream","Skin1004","Face","Moisturiser",19.99,7.10,"SKU-SKN-00023"),
    ("P0024","Round Lab Birch Juice Moisturiser","Round Lab","Face","Moisturiser",23.99,8.60,"SKU-RNL-00024"),
    ("P0025","Numbuzin No.3 Serum","Numbuzin","Face","Serum",34.99,12.70,"SKU-NUM-00025"),
    ("P0026","Biodance Bio-Collagen Mask","Biodance","Face","Sheet Mask",8.99,2.80,"SKU-BIO-00026"),
    ("P0027","Medipeel Glutathione Eye Cream","Medipeel","Eyes","Eye Mask",29.99,10.80,"SKU-MDP-00027"),
    ("P0028","Haruharu Black Rice Toner","Haruharu Wonder","Hair","Toner",21.99,7.80,"SKU-HRH-00028"),
    ("P0029","I'm From Mugwort Mask","I'm From","Face","Sheet Mask",24.99,8.90,"SKU-IMF-00029"),
    ("P0030","Etude House SoonJung Cream","Etude House","Face","Moisturiser",18.99,6.70,"SKU-ETH-00030"),
    # Hair
    ("P0031","COSRX Scalp Shampoo","COSRX","Hair","Shampoo",18.99,6.80,"SKU-COS-00031"),
    ("P0032","Laneige Damage Repair Conditioner","Laneige","Hair","Conditioner",22.99,8.20,"SKU-LAN-00032"),
    ("P0033","Innisfree Camellia Hair Ampoule","Innisfree","Hair","Treatment",26.99,9.80,"SKU-INN-00033"),
    ("P0034","Missha Bond Repair Shampoo","Missha","Hair","Shampoo",15.99,5.50,"SKU-MIS-00034"),
    ("P0035","The Face Shop Rice Conditioner","The Face Shop","Hair","Conditioner",14.99,5.10,"SKU-TFS-00035"),
    # Body
    ("P0036","Laneige Ceramide Body Lotion","Laneige","Body","Body Lotion",27.99,10.10,"SKU-LAN-00036"),
    ("P0037","Innisfree Tangerine Body Wash","Innisfree","Body","Body Wash",13.99,4.80,"SKU-INN-00037"),
    ("P0038","Some By Mi Body Scrub","Some By Mi","Body","Body Scrub",16.99,5.90,"SKU-SBM-00038"),
    # Tools
    ("P0039","Skin1004 Facial Roller","Skin1004","Tools","Skincare Tool",24.99,8.90,"SKU-SKN-00039"),
    ("P0040","Dr.Jart+ LED Mask","Dr.Jart+","Tools","Skincare Tool",79.99,29.00,"SKU-DRJ-00040"),
]

# product popularity weights — hero SKUs dominate (power law)
_raw_weights = np.random.power(0.4, len(PRODUCTS))
PRODUCT_WEIGHTS = (_raw_weights / _raw_weights.sum()).tolist()

COUNTRIES = {
    "DE": ("Germany",        0.22),
    "FR": ("France",         0.15),
    "GB": ("United Kingdom", 0.13),
    "NL": ("Netherlands",    0.09),
    "BE": ("Belgium",        0.07),
    "IT": ("Italy",          0.08),
    "ES": ("Spain",          0.07),
    "PL": ("Poland",         0.05),
    "SE": ("Sweden",         0.05),
    "AT": ("Austria",        0.04),
    "CH": ("Switzerland",    0.03),
    "DK": ("Denmark",        0.02),
}
COUNTRY_CODES   = list(COUNTRIES.keys())
COUNTRY_WEIGHTS = [v[1] for v in COUNTRIES.values()]
COUNTRY_NAMES   = {k: v[0] for k, v in COUNTRIES.items()}

CHANNELS    = ["web", "mobile_app"]
STATUSES    = ["completed","completed","completed","returned","cancelled"]
AGE_GROUPS  = ["18-24","25-34","35-44","45-54","55+"]
GENDERS     = ["F","F","F","M","prefer_not_to_say"]
LOYALTY     = ["bronze","silver","gold","platinum"]

# ──────────────────────────────────────────
# CUSTOMER POOL  (seeded once, reused daily)
# ──────────────────────────────────────────

def _build_customer_pool(n: int = 5000) -> list[dict]:
    pool = []
    for i in range(1, n + 1):
        cc = random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS)[0]
        pool.append({
            "customer_id":  f"C{i:06d}",
            "country_code": cc,
            "country":      COUNTRY_NAMES[cc],
            "city":         fake.city(),
            "age_group":    random.choices(AGE_GROUPS, weights=[20,35,25,12,8])[0],
            "gender":       random.choices(GENDERS)[0],
            "signup_date":  str(fake.date_between(
                                start_date="-2y", end_date="today")),
            "loyalty_tier": random.choices(LOYALTY, weights=[50,28,15,7])[0],
        })
    return pool

CUSTOMER_POOL = _build_customer_pool()

# ──────────────────────────────────────────
# SEASONALITY helpers
# ──────────────────────────────────────────

CAT_SEASON = {
    "Face":  [1.0,0.9,0.95,1.0,1.05,1.1,1.1,1.05,1.0,1.0,1.2,1.3],
    "Hair":  [0.9,0.9,1.0, 1.0,1.1, 1.2,1.2,1.1, 1.0,1.0,1.1,1.1],
    "Body":  [0.8,0.8,0.9, 1.0,1.1, 1.3,1.3,1.2, 1.0,0.9,1.0,1.1],
    "Lips":  [1.1,1.0,1.0, 1.0,1.0, 0.9,0.9,0.9, 1.0,1.0,1.2,1.3],
    "Eyes":  [1.0,1.0,1.0, 1.0,1.0, 1.0,1.0,1.0, 1.0,1.0,1.1,1.2],
    "Tools": [0.8,0.8,0.9, 0.9,1.0, 1.0,1.0,1.0, 1.0,1.1,1.2,1.4],
}


# ──────────────────────────────────────────
# CORE: generate one day's raw JSON payload
# ──────────────────────────────────────────

def generate_day(target_date: date, daily_mean: int = DAILY_ORDERS_MEAN) -> tuple[list[dict], dict]:
    """
    Returns:
        orders_payload  – list of nested order dicts  (→ raw/orders/date=.../orders.json)
        products_payload – dict with product list      (→ raw/products/snapshot_date=.../products.json)
    """
    month_idx  = target_date.month - 1
    is_weekend = target_date.weekday() >= 5
    base       = daily_mean * (1.25 if is_weekend else 1.0)
    n_orders   = max(1, int(np.random.poisson(base)))

    orders = []
    for oid in range(1, n_orders + 1):
        cust    = random.choice(CUSTOMER_POOL)
        channel = random.choices(CHANNELS, weights=[55, 45])[0]
        status  = random.choices(STATUSES)[0]

        # basket: 1–5 items
        basket_size = random.choices([1,2,3,4,5], weights=[35,30,20,10,5])[0]
        picked_idx  = np.random.choice(
            len(PRODUCTS), size=basket_size, replace=False, p=PRODUCT_WEIGHTS
        )

        items = []
        for idx in picked_idx:
            pid, name, brand, cat, subcat, unit_price, cost_price, sku = PRODUCTS[idx]
            season_w  = CAT_SEASON.get(cat, [1.0]*12)[month_idx]
            has_disc  = random.random() < 0.30
            disc_pct  = round(random.choice([5,10,15,20,25]) / 100, 2) if has_disc else 0.0
            sale_price = round(unit_price * season_w * (1 - disc_pct), 2)
            qty        = random.choices([1,2,3], weights=[70,22,8])[0]

            items.append({
                "line_item_id":    f"LI-{oid:06d}-{idx:03d}",
                "product_id":      pid,
                "sku":             sku,
                "product_name":    name,
                "brand":           brand,
                "category":        cat,
                "subcategory":     subcat,
                "quantity":        qty,
                "original_price":  unit_price,
                "sale_price":      sale_price,
                "discount_pct":    disc_pct,
                "cost_price":      cost_price,
                "currency":        "EUR",
            })

        # ISO-8601 timestamp with random hour/minute
        ts = datetime(
            target_date.year, target_date.month, target_date.day,
            random.randint(7, 23), random.randint(0, 59), random.randint(0, 59)
        )

        orders.append({
            "order_id":          f"ORD-{target_date.strftime('%Y%m%d')}-{oid:05d}",
            "created_at":        ts.isoformat() + "Z",
            "channel":           channel,
            "order_status":      status,
            "shipping_country":  cust["country_code"],
            "customer": {
                "customer_id":   cust["customer_id"],
                "country_code":  cust["country_code"],
                "country":       cust["country"],
                "city":          cust["city"],
                "age_group":     cust["age_group"],
                "gender":        cust["gender"],
                "loyalty_tier":  cust["loyalty_tier"],
                "signup_date":   cust["signup_date"],
            },
            "items": items,
        })

    # products snapshot (same every day unless prices change — useful for SCD tracking later)
    products_payload = {
        "snapshot_date": target_date.isoformat(),
        "source":        "kbeauty-store-api",
        "products": [
            {
                "product_id":   pid,
                "product_name": name,
                "brand":        brand,
                "category":     cat,
                "subcategory":  subcat,
                "unit_price":   unit_price,
                "cost_price":   cost_price,
                "sku":          sku,
                "is_active":    True,
                "updated_at":   target_date.isoformat() + "T00:00:00Z",
                "currency":     "EUR",
            }
            for pid, name, brand, cat, subcat, unit_price, cost_price, sku in PRODUCTS
        ],
    }

    return orders, products_payload


# ──────────────────────────────────────────
# GCS WRITER
# ──────────────────────────────────────────

def _gcs_client():
    if not GCS_AVAILABLE:
        raise RuntimeError("google-cloud-storage not installed. Run: pip install google-cloud-storage")
    return gcs.Client(project=GCS_PROJECT)


def write_to_gcs(target_date: date, orders: list[dict], products: dict, client=None):
    """
    Writes two blobs:
      raw/orders/date=YYYY-MM-DD/orders.json
      raw/products/snapshot_date=YYYY-MM-DD/products.json
    """
    client  = client or _gcs_client()
    bucket  = client.bucket(GCS_BUCKET)
    ds      = target_date.isoformat()

    orders_blob = bucket.blob(f"raw/orders/date={ds}/orders.json")
    orders_blob.upload_from_string(
        json.dumps(orders, ensure_ascii=False, indent=None),   # compact — smaller files
        content_type="application/json"
    )

    products_blob = bucket.blob(f"raw/products/snapshot_date={ds}/products.json")
    products_blob.upload_from_string(
        json.dumps(products, ensure_ascii=False, indent=None),
        content_type="application/json"
    )

    print(f"  ✓ GCS  gs://{GCS_BUCKET}/raw/orders/date={ds}/orders.json  "
          f"({len(orders)} orders)")


def write_to_local(target_date: date, orders: list[dict], products: dict):
    """Writes same layout locally for testing without GCP credentials."""
    import os
    ds = target_date.isoformat()

    orders_path   = f"{LOCAL_DIR}/raw/orders/date={ds}"
    products_path = f"{LOCAL_DIR}/raw/products/snapshot_date={ds}"
    os.makedirs(orders_path,   exist_ok=True)
    os.makedirs(products_path, exist_ok=True)

    with open(f"{orders_path}/orders.json",   "w") as f:
        json.dump(orders,   f, ensure_ascii=False)
    with open(f"{products_path}/products.json", "w") as f:
        json.dump(products, f, ensure_ascii=False)

    print(f"  ✓ Local {orders_path}/orders.json  ({len(orders)} orders)")


# ──────────────────────────────────────────
# RUN MODES
# ──────────────────────────────────────────

def run_seed(destination: str = "gcs"):
    """Backfill 1 year of daily files."""
    end_date   = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=364)
    current    = start_date

    client = _gcs_client() if destination == "gcs" else None

    print(f"\n🌸  K-Beauty Generator — SEED MODE  [{destination.upper()}]")
    print(f"    {start_date} → {end_date}  (~365 daily files)\n")

    total_orders = 0
    while current <= end_date:
        orders, products = generate_day(current)
        if destination == "gcs":
            write_to_gcs(current, orders, products, client)
        else:
            write_to_local(current, orders, products)
        total_orders += len(orders)
        current += timedelta(days=1)

    print(f"\n✅  Seed complete. {total_orders:,} orders across 365 days.\n")


def run_daily(destination: str = "gcs"):
    """
    Writes only yesterday's data.
    Called by Airflow every day at e.g. 02:00 UTC.
    """
    yesterday = date.today() - timedelta(days=1)
    client    = _gcs_client() if destination == "gcs" else None

    print(f"\n🌸  K-Beauty Generator — DAILY MODE  [{destination.upper()}]  {yesterday}")

    orders, products = generate_day(yesterday)
    if destination == "gcs":
        write_to_gcs(yesterday, orders, products, client)
    else:
        write_to_local(yesterday, orders, products)

    print(f"✅  Done. {len(orders)} orders written for {yesterday}\n")


# ──────────────────────────────────────────
# ENTRYPOINT
# ──────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="K-Beauty synthetic data generator")
    parser.add_argument(
        "mode", choices=["seed", "daily", "local"],
        help="seed=backfill 1yr to GCS | daily=yesterday to GCS | local=local filesystem"
    )
    args = parser.parse_args()

    if args.mode == "seed":
        run_seed(destination="gcs")
    elif args.mode == "daily":
        run_daily(destination="gcs")
    elif args.mode == "local":
        run_daily(destination="local")