# Placer AI x Retail | Site Intelligence Platform

**Placer AI Foot Traffic Data x ArcGIS JS API | Retail Site Intelligence | GeoEnrichment | Los Angeles**

## Live Demo

**[Launch the App](https://garridolecca.github.io/ArcGIS-and-Placer/)**

## Overview

A production-grade retail site intelligence platform that analyzes **1,156 retail locations** across **24 major brands** in the Los Angeles metropolitan area using **Placer AI foot traffic data**. The system combines real foot traffic patterns, dwell time analytics, visitor demographics, and ArcGIS GeoEnrichment to support real estate and site selection decisions.

## Key Features

- **Foot Traffic Analysis** - Visualize 17.2M+ weekly visits across LA metro retail locations
- **Multi-Brand Intelligence** - Compare 24 retail brands including Target, Costco, Whole Foods, Nordstrom, and more
- **Site Scoring** - Composite scores (0-100) weighting traffic, affluence, market size, and dwell time
- **GeoEnrichment Integration** - Real demographic data via ArcGIS API or simulated demo mode
- **Spatial Analysis** - Draw configurable radii (0.5-5 miles) around any location
- **Heatmap Visualization** - Toggle between point and heatmap views
- **Day-of-Week Patterns** - Weekday vs. weekend foot traffic analysis
- **Visitor Demographics** - Median household income from Placer AI visitor profiles
- **Publish to ArcGIS Online** - Upload analysis results as hosted feature services

## Data Source

- **Placer AI** foot traffic data (March 2025)
- **1,156 locations** across **24 brands** in the LA DMA
- Metrics include: weekly foot traffic, dwell time, unique visitors, visit frequency, visitor median HHI

## Brands Covered

99 Cents Only Stores, Albertsons, Bed Bath & Beyond, Best Buy, Big Lots, Burlington, Costco Wholesale, Dollar Tree, Five Below, HomeGoods, Kohl's, Lowe's, Macy's, Marshalls, Nordstrom, Nordstrom Rack, Ralphs, Ross Dress for Less, Target, The Home Depot, Trader Joe's, VONS, Walmart, Whole Foods Market

## Technology Stack

- **ArcGIS Maps SDK for JavaScript 4.30** (AMD modules)
- **Calcite Design System** (dark theme)
- **ArcGIS GeoEnrichment REST API**
- **GeoJSONLayer** with visual variables (size, color, opacity)
- Client-side spatial analysis via `geometryEngine.geodesicBuffer`

## Usage

1. Open the app via the live demo link or serve locally
2. Enter an ArcGIS API Key for live GeoEnrichment, or click "Explore with Demo Data"
3. Select a retail brand and store location from the dropdowns
4. Click "Analyze Store" to run site analysis with configurable radius
5. Enable "Click map to analyze custom location" for ad-hoc analysis
6. Toggle heatmap view for traffic density visualization

## File Structure

```
ArcGIS-and-Placer/
+-- index.html                     # Main application (single HTML file)
+-- data/
|   +-- placer_visits.geojson     # 1,156 retail location features
+-- aggregate_data.py              # S3 parquet to GeoJSON conversion
+-- README.md
```

## Languages

HTML, JavaScript, Python
