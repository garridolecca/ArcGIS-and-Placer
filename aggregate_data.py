"""
Placer AI S3 Parquet to GeoJSON Converter
Reads POI metadata and visit data from S3, filters for LA County retail chains,
and produces GeoJSON files for the ArcGIS web application.

Outputs:
  - data/placer_visits.geojson     -- POI points with visit stats + distance bands
  - data/placer_trade_areas.geojson -- CBG polygons classified as trade area rings

Data source: s3://geoanalytics-engine/data_packages/placer/converted/
"""

import os
import json
import tempfile
import zipfile
import io

import pandas as pd
import s3fs
import requests

try:
    import geopandas as gpd
    from shapely.geometry import mapping
    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False

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

# All 14 distance band suffixes used in Placer Premium Export columns
DISTANCE_BAND_SUFFIXES = [
    'less_than_0_3_miles',
    '0_3_1_miles',
    '1_2_miles',
    '2_3_miles',
    '3_5_miles',
    '5_7_miles',
    '7_10_miles',
    '10_15_miles',
    '15_25_miles',
    '25_50_miles',
    '50_100_miles',
    '100_150_miles',
    '150_250_miles',
    '250_plus_miles',
]

DISTANCE_BAND_FOOTTRAFFIC_COLS = [
    f'home_distance_estimated_foottraffic_{s}' for s in DISTANCE_BAND_SUFFIXES
]

DISTANCE_BAND_PERCENTAGE_COLS = [
    f'home_distance_percentage_{s}' for s in DISTANCE_BAND_SUFFIXES
]

# Census TIGER/Line shapefile URL for California (FIPS 06) block groups, 2022 vintage
TIGER_CBG_URL = 'https://www2.census.gov/geo/tiger/TIGER2022/BG/tl_2022_06_bg.zip'

# LA County FIPS county code
LA_COUNTY_FIPS = '037'

# Trade area classification thresholds (cumulative visit %)
PRIMARY_THRESHOLD = 0.60
SECONDARY_THRESHOLD = 0.80
TERTIARY_THRESHOLD = 0.95
MAX_CBGS_PER_POI = 50


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

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
    """Load visit data from S3 partitions and match to POI IDs.

    Includes distance band columns when available in the source data.
    """
    fs = s3fs.S3FileSystem(**{k: v for k, v in STORAGE_OPTS.items() if k != 'client_kwargs'},
                           client_kwargs=STORAGE_OPTS.get('client_kwargs', {}))
    parts = [f for f in fs.ls(f'{S3_BASE.replace("s3://", "")}/2025-03-31.parquet/')
             if f.endswith('.parquet')][:num_partitions]

    # Build the full column list: base cols + distance band cols
    all_cols = VISIT_COLS + DISTANCE_BAND_FOOTTRAFFIC_COLS + DISTANCE_BAND_PERCENTAGE_COLS

    all_visits = []
    for i, part in enumerate(parts):
        print(f"Reading visit partition {i + 1}/{len(parts)}...")
        try:
            df = pd.read_parquet(f's3://{part}', columns=all_cols, storage_options=STORAGE_OPTS)
        except Exception:
            # Fallback: some partitions may not have distance band columns yet
            print(f"  Partition {i + 1}: falling back to base columns (distance bands unavailable)")
            df = pd.read_parquet(f's3://{part}', columns=VISIT_COLS, storage_options=STORAGE_OPTS)

        matched = df[(df['id'].isin(poi_ids)) & (df['time_frame'] == 'weekly')]
        if len(matched) > 0:
            all_visits.append(matched)

    visits = pd.concat(all_visits, ignore_index=True)

    # Determine numeric columns (everything except id, name, time_frame)
    num_cols = [c for c in visits.columns if c not in ('id', 'name', 'time_frame')]
    for c in num_cols:
        visits[c] = pd.to_numeric(visits[c], errors='coerce')

    return visits.groupby('id')[num_cols].mean().reset_index()


# ---------------------------------------------------------------------------
# Origins feed
# ---------------------------------------------------------------------------

def load_origins_data(poi_ids):
    """Load visits-by-origin data from S3 and filter for target POIs.

    Returns a DataFrame with columns: id, region_code, visits
    filtered to visit_duration_segmentation == 'all_visits'.
    Returns None if the origins subfolder does not exist.
    """
    fs = s3fs.S3FileSystem(**{k: v for k, v in STORAGE_OPTS.items() if k != 'client_kwargs'},
                           client_kwargs=STORAGE_OPTS.get('client_kwargs', {}))

    # Check whether the origins subfolder exists
    base_no_scheme = S3_BASE.replace('s3://', '')
    try:
        listing = fs.ls(f'{base_no_scheme}/visits-by-origin/')
        parquet_files = [f for f in listing if f.endswith('.parquet')]
        if not parquet_files:
            raise FileNotFoundError
    except Exception:
        print("WARNING: visits-by-origin subfolder not found in S3. "
              "Skipping trade area generation.")
        return None

    print(f"Reading origins data ({len(parquet_files)} file(s))...")
    frames = []
    for pf in parquet_files:
        df = pd.read_parquet(
            f's3://{pf}',
            columns=['id', 'region_code', 'region_type', 'origin_type',
                     'visit_duration_segmentation', 'visits',
                     'start_date', 'end_date'],
            storage_options=STORAGE_OPTS
        )
        # Filter to all_visits segmentation and target POIs
        df = df[
            (df['visit_duration_segmentation'] == 'all_visits') &
            (df['id'].isin(poi_ids))
        ]
        if len(df) > 0:
            frames.append(df)

    if not frames:
        print("WARNING: No origin visits matched target POIs. Skipping trade areas.")
        return None

    origins = pd.concat(frames, ignore_index=True)
    origins['visits'] = pd.to_numeric(origins['visits'], errors='coerce')

    # Aggregate visits per POI-CBG pair across all time periods
    origins_agg = (
        origins.groupby(['id', 'region_code'])['visits']
        .sum()
        .reset_index()
    )
    print(f"Origins data loaded: {len(origins_agg)} POI-CBG pairs for "
          f"{origins_agg['id'].nunique()} POIs")
    return origins_agg


# ---------------------------------------------------------------------------
# Census Block Group boundaries
# ---------------------------------------------------------------------------

def download_cbg_boundaries():
    """Download and return LA County CBG boundaries.

    Primary method: download TIGER/Line shapefile via geopandas.
    Fallback: use Census Bureau TIGERweb GeoJSON REST API.

    Returns a GeoDataFrame (geopandas) or dict (fallback), or None on failure.
    """
    if HAS_GEOPANDAS:
        return _download_cbg_tiger_shapefile()
    else:
        return _download_cbg_geojson_api()


def _download_cbg_tiger_shapefile():
    """Download TIGER/Line shapefile for CA block groups, filter to LA County."""
    print("Downloading TIGER/Line CBG shapefile for California...")
    try:
        resp = requests.get(TIGER_CBG_URL, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR: Failed to download TIGER shapefile: {e}")
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        zf.extractall(tmpdir)
        # Find the .shp file
        shp_files = [f for f in os.listdir(tmpdir) if f.endswith('.shp')]
        if not shp_files:
            print("ERROR: No .shp file found in TIGER download.")
            return None

        shp_path = os.path.join(tmpdir, shp_files[0])
        print(f"Reading shapefile {shp_files[0]}...")
        gdf = gpd.read_file(shp_path)

    # Filter for LA County (COUNTYFP == '037')
    la_cbgs = gdf[gdf['COUNTYFP'] == LA_COUNTY_FIPS].copy()
    la_cbgs = la_cbgs.to_crs(epsg=4326)  # ensure WGS84
    print(f"Loaded {len(la_cbgs)} CBG boundaries for LA County")
    return la_cbgs[['GEOID', 'geometry', 'ALAND']].copy()


def _download_cbg_geojson_api():
    """Fallback: fetch LA County CBG boundaries from TIGERweb REST API."""
    print("geopandas not available. Using Census TIGERweb GeoJSON API (fallback)...")
    base_url = (
        'https://tigerweb.geo.census.gov/arcgis/rest/services/'
        'TIGERweb/tigerWMS_ACS2022/MapServer/10/query'
    )
    params = {
        'where': "STATE='06' AND COUNTY='037'",
        'outFields': 'GEOID,ALAND',
        'outSR': '4326',
        'f': 'geojson',
        'returnGeometry': 'true',
        'resultRecordCount': 5000,
    }
    try:
        resp = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"ERROR: TIGERweb API request failed: {e}")
        return None

    features = data.get('features', [])
    if not features:
        print("ERROR: No CBG features returned from TIGERweb API.")
        return None

    print(f"Loaded {len(features)} CBG boundaries from TIGERweb API (fallback)")
    return {
        'type': 'geojson_fallback',
        'features': features
    }


# ---------------------------------------------------------------------------
# Trade area generation
# ---------------------------------------------------------------------------

def classify_trade_areas(origins_agg, cbg_boundaries, pois):
    """Build trade area GeoJSON from origins data joined to CBG boundaries.

    For each POI, CBGs are ranked by visit count and classified into
    primary / secondary / tertiary rings based on cumulative visit share.

    Returns a GeoJSON FeatureCollection dict.
    """
    poi_name_map = dict(zip(pois['id'], pois['name']))
    features = []

    # Prepare CBG lookup depending on whether we have geopandas or fallback
    if HAS_GEOPANDAS and hasattr(cbg_boundaries, 'set_index'):
        cbg_lookup = cbg_boundaries.set_index('GEOID')
    else:
        cbg_lookup = {}
        if isinstance(cbg_boundaries, dict) and cbg_boundaries.get('type') == 'geojson_fallback':
            for feat in cbg_boundaries['features']:
                geoid = feat['properties'].get('GEOID', '')
                cbg_lookup[geoid] = feat

    poi_ids = origins_agg['id'].unique()
    print(f"Generating trade areas for {len(poi_ids)} POIs...")

    for poi_id in poi_ids:
        poi_origins = origins_agg[origins_agg['id'] == poi_id].copy()
        poi_origins = poi_origins.sort_values('visits', ascending=False)

        total_visits = poi_origins['visits'].sum()
        if total_visits <= 0:
            continue

        poi_origins['visit_pct'] = poi_origins['visits'] / total_visits
        poi_origins['cumulative_pct'] = poi_origins['visit_pct'].cumsum()

        # Keep only CBGs up to TERTIARY_THRESHOLD or MAX_CBGS_PER_POI
        mask = (poi_origins['cumulative_pct'].shift(1, fill_value=0) < TERTIARY_THRESHOLD)
        top_cbgs = poi_origins[mask].head(MAX_CBGS_PER_POI)

        poi_name = poi_name_map.get(poi_id, '')

        for _, row in top_cbgs.iterrows():
            cbg_fips = str(row['region_code'])
            cum_pct = float(row['cumulative_pct'])

            # Classify ring
            if cum_pct <= PRIMARY_THRESHOLD:
                ring = 'primary'
            elif cum_pct <= SECONDARY_THRESHOLD:
                ring = 'secondary'
            else:
                ring = 'tertiary'

            # Look up geometry
            geometry = None
            population = 0
            if HAS_GEOPANDAS and hasattr(cbg_boundaries, 'set_index'):
                if cbg_fips in cbg_lookup.index:
                    cbg_row = cbg_lookup.loc[cbg_fips]
                    geometry = mapping(cbg_row.geometry)
                    population = int(cbg_row.get('ALAND', 0)) if 'ALAND' in cbg_lookup.columns else 0
            else:
                if cbg_fips in cbg_lookup:
                    feat = cbg_lookup[cbg_fips]
                    geometry = feat.get('geometry')
                    population = feat['properties'].get('ALAND', 0)

            if geometry is None:
                continue

            props = {
                'cbg_fips': cbg_fips,
                'poi_id': poi_id,
                'poi_name': poi_name,
                'visits': int(row['visits']),
                'visit_pct': round(float(row['visit_pct']), 4),
                'cumulative_pct': round(cum_pct, 4),
                'trade_area_ring': ring,
                'population': population,
            }
            features.append({
                'type': 'Feature',
                'geometry': geometry,
                'properties': props,
            })

    print(f"Trade area GeoJSON: {len(features)} CBG features across {len(poi_ids)} POIs")
    return {'type': 'FeatureCollection', 'features': features}


# ---------------------------------------------------------------------------
# GeoJSON builders
# ---------------------------------------------------------------------------

def build_geojson(pois, visits):
    """Merge POI metadata with visit aggregates and output GeoJSON.

    Includes distance band properties when available in the visit data.
    """
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

        # Add distance band foottraffic columns (shortened property names for GeoJSON)
        band_labels = [
            'dist_band_lt_0_3_mi', 'dist_band_0_3_1_mi', 'dist_band_1_2_mi',
            'dist_band_2_3_mi', 'dist_band_3_5_mi', 'dist_band_5_7_mi',
            'dist_band_7_10_mi', 'dist_band_10_15_mi', 'dist_band_15_25_mi',
            'dist_band_25_50_mi', 'dist_band_50_100_mi', 'dist_band_100_150_mi',
            'dist_band_150_250_mi', 'dist_band_250_plus_mi'
        ]
        pct_labels = [
            'dist_pct_lt_0_3_mi', 'dist_pct_0_3_1_mi', 'dist_pct_1_2_mi',
            'dist_pct_2_3_mi', 'dist_pct_3_5_mi', 'dist_pct_5_7_mi',
            'dist_pct_7_10_mi', 'dist_pct_10_15_mi', 'dist_pct_15_25_mi',
            'dist_pct_25_50_mi', 'dist_pct_50_100_mi', 'dist_pct_100_150_mi',
            'dist_pct_150_250_mi', 'dist_pct_250_plus_mi'
        ]

        for short_name, col in zip(band_labels, DISTANCE_BAND_FOOTTRAFFIC_COLS):
            if col in row.index and pd.notna(row[col]):
                props[short_name] = round(float(row[col]))
            else:
                props[short_name] = 0

        for short_name, col in zip(pct_labels, DISTANCE_BAND_PERCENTAGE_COLS):
            if col in row.index and pd.notna(row[col]):
                props[short_name] = round(float(row[col]), 4)
            else:
                props[short_name] = 0.0

        features.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [lng, lat]},
            'properties': props
        })

    return {'type': 'FeatureCollection', 'features': features}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    # Ensure output directory exists
    os.makedirs('data', exist_ok=True)

    # --- Step 1: POI metadata ---
    pois = load_poi_metadata()
    poi_ids = set(pois['id'].tolist())

    # --- Step 2: Visit data (with distance bands) ---
    visits = load_visit_data(poi_ids, num_partitions=5)
    print(f"Visit data aggregated for {len(visits)} POIs")

    # --- Step 3: Build main POI GeoJSON ---
    geojson = build_geojson(pois, visits)
    visits_output = 'data/placer_visits.geojson'
    with open(visits_output, 'w') as f:
        json.dump(geojson, f)
    print(f"GeoJSON written to {visits_output} with {len(geojson['features'])} features")

    # --- Step 4: Origins + Trade Areas (skip gracefully if unavailable) ---
    origins = load_origins_data(poi_ids)

    if origins is not None:
        cbg_boundaries = download_cbg_boundaries()
        if cbg_boundaries is not None:
            trade_area_geojson = classify_trade_areas(origins, cbg_boundaries, pois)
            trade_area_output = 'data/placer_trade_areas.geojson'
            with open(trade_area_output, 'w') as f:
                json.dump(trade_area_geojson, f)
            print(f"Trade area GeoJSON written to {trade_area_output} "
                  f"with {len(trade_area_geojson['features'])} features")
        else:
            print("WARNING: Could not load CBG boundaries. Skipping trade area output.")
    else:
        print("Trade area generation skipped (no origins data).")

    print("Done.")
