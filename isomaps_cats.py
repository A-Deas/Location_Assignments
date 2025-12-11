import duckdb
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.cm import ScalarMappable
from shapely.geometry import Polygon
import h3
import random

# --- Paths ---
DRIVETIME_DUCKDB = "Data/utah_drivetimes.duckdb"
PLACES_SHP_PATH = "Shapefiles/cb_2019_49_place_500k/cb_2019_49_place_500k.shp"
UTAH_SHP_PATH = "Shapefiles/tl_2019_49_tract/tl_2019_49_tract.shp"

# --- Isochrone thresholds ---
THRESHOLDS = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]

# === Helper functions ===

def h3_to_polygon(hex_id):
    coords = h3.cell_to_boundary(hex_id)
    lon_lat = [(lon, lat) for lat, lon in coords]
    return Polygon(lon_lat)

def build_utah_hex_gdf(utah_boundary, resolution=8):
    """Generate hex grid over Utah and clip to boundary."""
    minx, miny, maxx, maxy = utah_boundary.total_bounds
    
    lat_lng_pairs = [
        (lat, lon)
        for lat in np.arange(miny, maxy, 0.005)
        for lon in np.arange(minx, maxx, 0.005)
    ]
    all_hexes = {h3.latlng_to_cell(lat, lon, resolution) for lat, lon in lat_lng_pairs}
    
    gdf = gpd.GeoDataFrame(
        {"dest_h3": list(all_hexes)},
        geometry=[h3_to_polygon(h) for h in all_hexes],
        crs="EPSG:4326"
    )
    
    # Optionally clip to Utah boundary to trim spillover
    # gdf = gpd.overlay(gdf, utah_boundary, how="intersection")
    return gdf

def plot_isochrone_map(gdf, utah_boundary, places, source_h3, thresholds):
    fig, ax = plt.subplots(figsize=(8, 8))
    utah_boundary.plot(ax=ax, color="lightgrey", edgecolor="black", linewidth=0.5)

    # --- Categorical coloring ---
    cmap = plt.get_cmap("YlOrRd", len(thresholds) + 1)  # discrete colormap
    gdf.plot(
        ax=ax,
        column="duration_bin",
        cmap=cmap,
        linewidth=0.05,
        edgecolor="white",
        alpha=0.85,
        legend=True,
        legend_kwds={"title": "Drivetime (sec)", "loc": "lower left", "fontsize": 8, "title_fontsize": 9},
        missing_kwds={"color": "lightgrey"}
    )

    # Mark the source hex
    source_poly = h3_to_polygon(source_h3)
    gpd.GeoSeries([source_poly], crs="EPSG:4326").plot(ax=ax, color="blue", alpha=0.8, label="Source")

    # Overlay places
    places.boundary.plot(ax=ax, color="grey", linewidth=0.5, alpha=0.5)

    ax.set_title(f"Isochrone Map — Source H3: {source_h3}", fontsize=12, weight='bold')
    ax.axis('off')
    plt.tight_layout()
    plt.show()

def main():
    # --- Load shapefiles ---
    utah = gpd.read_file(UTAH_SHP_PATH).to_crs("EPSG:4326")
    places = gpd.read_file(PLACES_SHP_PATH).to_crs("EPSG:4326")
    utah_boundary = utah.dissolve()

    # --- Connect to DuckDB ---
    con = duckdb.connect(DRIVETIME_DUCKDB, read_only=True)

    # --- Build the canonical Utah hex grid ---
    utah_hex_gdf = build_utah_hex_gdf(utah_boundary)
    utah_hexes = set(utah_hex_gdf["dest_h3"])  # all in-state hexes

    # --- Choose random source ---
    sources = con.execute("SELECT DISTINCT source_h3 FROM utah_drivetimes").fetchall()
    sources = [s[0] for s in sources if s[0] in utah_hexes]
    
    source_h3 = random.choice(sources)
    print(f"Selected random source within Utah: {source_h3}")

    # --- Query drivetime results ---
    df = con.execute(f"""
        SELECT dest_h3, duration
        FROM utah_drivetimes
        WHERE source_h3 = '{source_h3}' AND duration <= {max(THRESHOLDS)}
    """).df()

    # --- Merge durations into the Utah hex grid ---
    merged = utah_hex_gdf.merge(df, on="dest_h3", how="left")

    # --- Drop unreachable or zero-duration hexes ---
    filtered = merged[(merged["duration"].notnull()) & (merged["duration"] > 0)].copy()

    # --- Categorize durations into threshold bins ---
    bins = [0] + THRESHOLDS + [float("inf")]
    labels = [f"{bins[i]}–{bins[i+1]}" for i in range(len(bins)-2)] + [f">{THRESHOLDS[-1]}"]
    filtered["duration_bin"] = pd.cut(filtered["duration"], bins=bins, labels=labels, right=True)


    # --- Plot ---
    plot_isochrone_map(filtered, utah_boundary, places, source_h3, THRESHOLDS)

if __name__ == "__main__":
    main()