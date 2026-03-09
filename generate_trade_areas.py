"""
Generate placer_trade_areas.geojson using real Census Bureau CBG boundaries
and the existing placer_visits.geojson POI data.

Downloads LA County CBG polygons from Census TIGERweb REST API, then for each
POI assigns nearby CBGs as trade area origins with realistic visit distributions
based on distance decay from the store location.
"""

import json
import math
import urllib.request
import sys
import os

# Trade area classification thresholds
PRIMARY_THRESHOLD = 0.60
SECONDARY_THRESHOLD = 0.80
TERTIARY_THRESHOLD = 0.95
MAX_CBGS_PER_POI = 25


def simplify_ring(ring):
    """Simplify a coordinate ring: reduce precision and downsample points."""
    if len(ring) <= 4:
        return [[round(p[0], 3), round(p[1], 3)] for p in ring]

    # Round coordinates (3 decimal places ~ 111m precision, good for CBG-level viz)
    rounded = [[round(p[0], 3), round(p[1], 3)] for p in ring]

    # Remove duplicate consecutive points
    simplified = [rounded[0]]
    for i in range(1, len(rounded)):
        if rounded[i] != simplified[-1]:
            simplified.append(rounded[i])

    # Keep every Nth point for large rings
    if len(simplified) > 12:
        step = max(2, len(simplified) // 10)
        kept = [simplified[0]]
        for i in range(step, len(simplified) - 1, step):
            kept.append(simplified[i])
        kept.append(simplified[-1])
        if kept[0] != kept[-1]:
            kept.append(kept[0])
        return kept

    return simplified


def simplify_geometry(geom):
    """Simplify polygon/multipolygon geometry to reduce file size."""
    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    if gtype == "Polygon":
        new_coords = [simplify_ring(ring) for ring in coords]
        return {"type": "Polygon", "coordinates": new_coords}
    elif gtype == "MultiPolygon":
        new_coords = []
        for poly in coords:
            new_coords.append([simplify_ring(ring) for ring in poly])
        return {"type": "MultiPolygon", "coordinates": new_coords}
    return geom


def haversine_miles(lat1, lon1, lat2, lon2):
    """Calculate distance in miles between two lat/lon points."""
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def centroid_of_polygon(coords):
    """Approximate centroid of a polygon (first ring only)."""
    ring = coords[0] if coords else []
    if not ring:
        return None, None
    lats = [p[1] for p in ring]
    lons = [p[0] for p in ring]
    return sum(lats)/len(lats), sum(lons)/len(lons)


def centroid_of_multipolygon(coords):
    """Approximate centroid of a multipolygon."""
    all_lats, all_lons = [], []
    for poly in coords:
        ring = poly[0] if poly else []
        all_lats.extend(p[1] for p in ring)
        all_lons.extend(p[0] for p in ring)
    if not all_lats:
        return None, None
    return sum(all_lats)/len(all_lats), sum(all_lons)/len(all_lons)


def fetch_cbg_boundaries():
    """Fetch LA County CBG boundaries from Census TIGERweb REST API.

    Paginate through results since the API has a max record limit.
    """
    base_url = (
        "https://tigerweb.geo.census.gov/arcgis/rest/services/"
        "TIGERweb/tigerWMS_ACS2022/MapServer/8/query"
    )

    all_features = []
    offset = 0
    batch_size = 500

    while True:
        params = (
            f"where=STATE%3D%2706%27+AND+COUNTY%3D%27037%27"
            f"&outFields=GEOID,AREALAND,CENTLAT,CENTLON"
            f"&outSR=4326"
            f"&f=geojson"
            f"&returnGeometry=true"
            f"&resultRecordCount={batch_size}"
            f"&resultOffset={offset}"
        )
        url = f"{base_url}?{params}"
        print(f"  Fetching CBGs offset={offset}...")

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        print(f"  Got {len(features)} features (total: {len(all_features)})")

        if len(features) < batch_size:
            break
        offset += batch_size

    return all_features


def precompute_cbg_centroids(cbg_features):
    """Extract centroid and metadata for each CBG feature.

    Uses CENTLAT/CENTLON from the API when available, falls back to
    computing centroid from polygon coordinates.
    """
    centroids = []
    for feat in cbg_features:
        props = feat.get("properties", {})
        geoid = props.get("GEOID", "")
        aland = props.get("AREALAND", 0) or 0

        # Use API-provided centroids if available
        clat = props.get("CENTLAT")
        clon = props.get("CENTLON")
        if clat and clon:
            try:
                lat, lon = float(clat), float(clon)
            except (ValueError, TypeError):
                lat, lon = None, None
        else:
            # Fallback: compute from geometry
            geom = feat.get("geometry", {})
            gtype = geom.get("type", "")
            coords = geom.get("coordinates", [])
            if gtype == "Polygon":
                lat, lon = centroid_of_polygon(coords)
            elif gtype == "MultiPolygon":
                lat, lon = centroid_of_multipolygon(coords)
            else:
                lat, lon = None, None

        if lat is not None:
            # Estimate population from land area (avg LA County density ~2700/sq mi)
            area_sq_mi = aland / 2_589_988 if aland > 0 else 0.1
            est_pop = max(100, int(area_sq_mi * 2700))

            centroids.append({
                "feature": feat,
                "lat": lat,
                "lon": lon,
                "geoid": geoid,
                "population": est_pop,
                "aland": aland
            })
    return centroids


def generate_trade_area_for_poi(poi, cbg_centroids):
    """Generate trade area CBG assignments for a single POI.

    Uses distance-decay model: visits ~ foottraffic * exp(-distance/decay_factor)
    with some randomness seeded by the POI's coordinates.
    """
    poi_coords = poi["geometry"]["coordinates"]
    poi_lon, poi_lat = poi_coords[0], poi_coords[1]
    props = poi["properties"]
    poi_id = props.get("placer_id", "")
    poi_name = props.get("name", "")
    foottraffic = props.get("avg_weekly_foottraffic", 1000)

    # Seed for deterministic randomness
    seed = abs(hash(poi_id)) if poi_id else int(abs(poi_lat * 10000 + poi_lon * 10000))

    # Calculate distance to each CBG and assign visits via distance decay
    # Decay factor varies by store type (superstores draw from farther)
    cat = props.get("category_group", "")
    if cat in ("Superstores", "Electronics", "Home Improvements & Furnishings"):
        decay_miles = 8.0
    elif cat == "Groceries":
        decay_miles = 4.0
    else:
        decay_miles = 6.0

    cbg_visits = []
    for cbg in cbg_centroids:
        dist = haversine_miles(poi_lat, poi_lon, cbg["lat"], cbg["lon"])
        if dist > 50:  # Skip CBGs beyond 50 miles
            continue

        # Distance decay with population weight
        pop_factor = max(0.1, min(3.0, cbg["population"] / 2000))
        raw_visits = foottraffic * math.exp(-dist / decay_miles) * pop_factor * 0.01

        # Add deterministic variation based on CBG GEOID + POI
        variation = ((hash(cbg["geoid"] + poi_id) % 100) / 100) * 0.6 + 0.7
        visits = max(1, round(raw_visits * variation))

        if visits >= 5:  # Only include meaningful visit counts
            cbg_visits.append({
                "geoid": cbg["geoid"],
                "visits": visits,
                "population": cbg["population"],
                "feature": cbg["feature"]
            })

    if not cbg_visits:
        return []

    # Sort by visits descending
    cbg_visits.sort(key=lambda x: x["visits"], reverse=True)

    # Calculate cumulative percentages and classify rings
    total_visits = sum(c["visits"] for c in cbg_visits)
    if total_visits <= 0:
        return []

    features = []
    cumulative = 0
    for i, cbg in enumerate(cbg_visits):
        if i >= MAX_CBGS_PER_POI:
            break

        visit_pct = cbg["visits"] / total_visits
        cumulative += visit_pct

        if cumulative <= PRIMARY_THRESHOLD:
            ring = "primary"
        elif cumulative <= SECONDARY_THRESHOLD:
            ring = "secondary"
        elif cumulative <= TERTIARY_THRESHOLD:
            ring = "tertiary"
        else:
            break  # Stop after tertiary threshold

        features.append({
            "type": "Feature",
            "geometry": simplify_geometry(cbg["feature"]["geometry"]),
            "properties": {
                "cbg_fips": cbg["geoid"],
                "poi_id": poi_id,
                "poi_name": poi_name,
                "visits": cbg["visits"],
                "visit_pct": round(visit_pct, 4),
                "cumulative_pct": round(cumulative, 4),
                "trade_area_ring": ring,
                "population": cbg["population"]
            }
        })

    return features


def main():
    # Load existing POI data
    print("Loading placer_visits.geojson...")
    with open("data/placer_visits.geojson", "r") as f:
        pois = json.load(f)

    poi_features = pois["features"]
    print(f"Loaded {len(poi_features)} POI features")

    # Fetch CBG boundaries
    print("Downloading LA County CBG boundaries from Census Bureau...")
    cbg_features = fetch_cbg_boundaries()
    print(f"Downloaded {len(cbg_features)} CBG polygons")

    if not cbg_features:
        print("ERROR: No CBG features downloaded. Check internet connection.")
        sys.exit(1)

    # Precompute centroids
    print("Computing CBG centroids...")
    cbg_centroids = precompute_cbg_centroids(cbg_features)
    print(f"Computed centroids for {len(cbg_centroids)} CBGs")

    # Generate trade areas for each POI
    print("Generating trade areas...")
    all_trade_area_features = []
    for i, poi in enumerate(poi_features):
        if (i + 1) % 100 == 0:
            print(f"  Processing POI {i + 1}/{len(poi_features)}...")

        ta_features = generate_trade_area_for_poi(poi, cbg_centroids)
        all_trade_area_features.extend(ta_features)

    print(f"Generated {len(all_trade_area_features)} trade area CBG features")

    # Count rings
    rings = {"primary": 0, "secondary": 0, "tertiary": 0}
    for f in all_trade_area_features:
        ring = f["properties"]["trade_area_ring"]
        rings[ring] = rings.get(ring, 0) + 1
    print(f"  Primary: {rings['primary']}, Secondary: {rings['secondary']}, Tertiary: {rings['tertiary']}")

    # Write output
    output = {
        "type": "FeatureCollection",
        "features": all_trade_area_features
    }

    output_path = "data/placer_trade_areas.geojson"
    with open(output_path, "w") as f:
        json.dump(output, f)

    # File size
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Written to {output_path} ({size_mb:.1f} MB)")
    print("Done.")


if __name__ == "__main__":
    main()
