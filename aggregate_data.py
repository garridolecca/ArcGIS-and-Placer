"""
Placer AI S3 Parquet to GeoJSON Converter
Reads POI metadata and visit data from S3, filters for LA County retail chains,
and produces a GeoJSON file for the ArcGIS web application.

Data source: s3://geoanalytics-engine/data_packages/placer/converted/
"""

import pandas as pd
import s3fs
import json

STORAGE_OPTS = {
    'key': 'YOUR_ACCESS_KEY',
    'secret': 'YOUR_SECRET_KEY',
    'client_kwargs': {'region_name': 'us-east-1'}
}

S3_BASE = 's3://geoanalytics-engine/data_packages/placer/converted'

TARGET_BRANDS = [
    'Target', 'Walmart', 'Costco Wholesale', 'Ross Dress for Less',
    'Nordstrom Rack', 'Marshalls', 'TJ Maxx', 'The Home Depot',
    'Ralphs', 'VONS', "Trader Joe's", 'Whole Foods Market',
    'Albertsons', 'Dollar Tree', '99 Cents Only Stores',
    'Nordstrom', 'Best Buy', "Lowe's", 'Bed Bath & Beyond',
    "Kohl's", "Macy's", 'HomeGoods', 'Burlington',
    'Big Lots', 'Five Below', 'Grocery Outlet'
]

RETAIL_CATEGORIES = [
    'Apparel', 'Shops & Services', 'Groceries', 'Superstores',
    'Home Improvements & Furnishings', 'Electronics'
]

VISIT_COLS = [
    'id', 'name', 'time_frame', 'foottraffic', 'avg_dwell_time',
    'unique_visitors', 'avg_visits_frequency', 'census_captured_median_hhi',
    'visits_by_day_of_week_monday', 'visits_by_day_of_week_saturday',
    'visits_by_day_of_week_sunday'
]


def load_poi_metadata():
    """Load all POI metadata and filter for LA retail chains."""
    print("Reading POI metadata...")
    df = pd.read_parquet(f'{S3_BASE}/poi_metadata.parquet/', storage_options=STORAGE_OPTS)
    la = df[df['dma_name'].str.contains('Los Angeles', na=False)]
    la_retail = la[la['category_group'].isin(RETAIL_CATEGORIES)]
    la_chains = la_retail[la_retail['name'].isin(TARGET_BRANDS)]
    print(f"Found {len(la_chains)} retail chain locations in LA DMA")
    return la_chains


def load_visit_data(poi_ids, num_partitions=5):
    """Load visit data from S3 partitions and match to POI IDs."""
    fs = s3fs.S3FileSystem(**{k: v for k, v in STORAGE_OPTS.items() if k != 'client_kwargs'},
                           client_kwargs=STORAGE_OPTS.get('client_kwargs', {}))
    parts = [f for f in fs.ls(f'{S3_BASE.replace("s3://", "")}/2025-03-31.parquet/')
             if f.endswith('.parquet')][:num_partitions]

    all_visits = []
    for i, part in enumerate(parts):
        print(f"Reading visit partition {i + 1}/{len(parts)}...")
        df = pd.read_parquet(f's3://{part}', columns=VISIT_COLS, storage_options=STORAGE_OPTS)
        matched = df[(df['id'].isin(poi_ids)) & (df['time_frame'] == 'weekly')]
        if len(matched) > 0:
            all_visits.append(matched)

    visits = pd.concat(all_visits, ignore_index=True)
    num_cols = [c for c in VISIT_COLS if c not in ('id', 'name', 'time_frame')]
    for c in num_cols:
        visits[c] = pd.to_numeric(visits[c], errors='coerce')

    return visits.groupby('id')[num_cols].mean().reset_index()


def build_geojson(pois, visits):
    """Merge POI metadata with visit aggregates and output GeoJSON."""
    merged = pois.merge(visits, on='id', how='inner')
    merged = merged[merged['foottraffic'] > 0]

    features = []
    for _, row in merged.iterrows():
        try:
            lat, lng = float(row['lat']), float(row['lng'])
        except (ValueError, TypeError):
            continue

        props = {
            'name': row['name'],
            'category': str(row.get('category', '')),
            'sub_category': str(row.get('sub_category', '')),
            'category_group': str(row.get('category_group', '')),
            'address': str(row.get('address', '')),
            'city': str(row.get('city', '')),
            'state': str(row.get('state_code', 'CA')),
            'zipcode': str(row.get('zipcode', '')),
            'avg_weekly_foottraffic': round(float(row['foottraffic'])),
            'avg_dwell_time': round(float(row['avg_dwell_time']), 1) if pd.notna(row['avg_dwell_time']) else 0,
            'avg_unique_visitors': round(float(row['unique_visitors'])) if pd.notna(row['unique_visitors']) else 0,
            'avg_visit_frequency': round(float(row['avg_visits_frequency']), 2) if pd.notna(row['avg_visits_frequency']) else 0,
            'median_hhi': round(float(row['census_captured_median_hhi'])) if pd.notna(row['census_captured_median_hhi']) else 0,
            'weekday_visits': round(float(row['visits_by_day_of_week_monday'])) if pd.notna(row['visits_by_day_of_week_monday']) else 0,
            'saturday_visits': round(float(row['visits_by_day_of_week_saturday'])) if pd.notna(row['visits_by_day_of_week_saturday']) else 0,
            'sunday_visits': round(float(row['visits_by_day_of_week_sunday'])) if pd.notna(row['visits_by_day_of_week_sunday']) else 0,
            'placer_id': row['id']
        }
        features.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [lng, lat]},
            'properties': props
        })

    return {'type': 'FeatureCollection', 'features': features}


if __name__ == '__main__':
    pois = load_poi_metadata()
    poi_ids = set(pois['id'].tolist())

    visits = load_visit_data(poi_ids, num_partitions=5)
    print(f"Visit data aggregated for {len(visits)} POIs")

    geojson = build_geojson(pois, visits)
    output_path = 'data/placer_visits.geojson'
    with open(output_path, 'w') as f:
        json.dump(geojson, f)

    print(f"GeoJSON written to {output_path} with {len(geojson['features'])} features")
