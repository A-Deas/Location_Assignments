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

def safe_h3_distance(src, dst):
    try:
        return h3.grid_distance(src, dst)  # H3 v4 API
    except Exception:
        return None

def max_k_per_bin(source_h3, df_with_bins):
    """Return dict {label -> max H3 grid distance} only from observed dests."""
    tmp = df_with_bins[["dest_h3", "duration_bin"]].dropna(subset=["duration_bin"]).copy()
    tmp["h3dist"] = tmp["dest_h3"].apply(lambda d: safe_h3_distance(source_h3, d))
    tmp = tmp.dropna(subset=["h3dist"])
    grouped = tmp.groupby("duration_bin")["h3dist"].max()
    grouped = grouped.dropna()  # remove NaN groups
    return grouped.astype(int).to_dict()

def reconstruct_ring_labels(source_h3, labels_in_order, bin_to_k):
    """
    Build concentric H3 rings from source_h3 using the max k per bin.
    Returns DataFrame with columns: dest_h3, duration_bin (reconstructed).
    """
    assigned = set()
    rows = []
    for label in labels_in_order:
        k = int(bin_to_k.get(label, 0) or 0)
        if k <= 0:
            continue
        disk = set(h3.grid_disk(source_h3, k))   # all cells within k (inclusive)
        ring = disk - assigned                   # remove inner disk(s)
        assigned |= disk
        if not ring:
            continue
        for h in ring:
            rows.append({"dest_h3": h, "duration_bin": label})
    return pd.DataFrame(rows, columns=["dest_h3", "duration_bin"])

def plot_iterative_rings(rings_gdf_all, fragmented_gdf, utah_boundary, places, source_h3, thresholds, labels):
    """
    Produce one plot per ring, cumulatively adding rings.
    """
    cmap = plt.get_cmap("RdYlGn_r", len(thresholds) + 1)

    # Ensure correct ordering
    rings_gdf_all["duration_bin"] = pd.Categorical(
        rings_gdf_all["duration_bin"],
        categories=labels,
        ordered=True
    )

    # Group rings by category
    ring_groups = {
        label: rings_gdf_all[rings_gdf_all["duration_bin"] == label]
        for label in labels
    }

    cumulative = gpd.GeoDataFrame(columns=rings_gdf_all.columns, geometry="geometry", crs="EPSG:4326")

    for idx, label in enumerate(labels):
        # Add this ring to cumulative set
        cumulative = pd.concat([cumulative, ring_groups[label]], ignore_index=True)

        fig, ax = plt.subplots(figsize=(8, 8))
        utah_boundary.plot(ax=ax, color="lightgrey", edgecolor="black", linewidth=0.5)

        # Plot cumulative rings
        if not cumulative.empty:
            cumulative.plot(
                ax=ax,
                column="duration_bin",
                cmap=cmap,
                linewidth=0.05,
                edgecolor="white",
                alpha=0.90,
                legend=False,
                missing_kwds={"color": "lightgrey"},
            )

        # Always show the fragmented observed data for context
        fragmented_gdf.plot(
            ax=ax,
            column="duration_bin",
            cmap=cmap,
            linewidth=0.2,
            edgecolor="black",
            alpha=1,   # faint overlay
            legend=True,
            legend_kwds={"title": "Drivetime (sec)", "loc": "lower left", "fontsize": 8, "title_fontsize": 9},
            missing_kwds={"color": "lightgrey"},
        )

        # Source hex
        src_poly = h3_to_polygon(source_h3)
        gpd.GeoSeries([src_poly], crs="EPSG:4326").plot(ax=ax, color="blue", alpha=0.9)

        places.boundary.plot(ax=ax, color="grey", linewidth=0.5, alpha=0.5)

        ax.set_title(f"Isochrone Rings Up to: {label}", fontsize=12, weight="bold")
        ax.axis("off")

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
    
    # source_h3 = random.choice(sources)
    source_h3 = "8826956323fffff"
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
    bins = [0] + THRESHOLDS  # stop at 5000
    labels = [f"{bins[i]}–{bins[i+1]}" for i in range(len(bins)-1)]

    # --- Categorize durations into threshold bins ---
    filtered["duration_bin"] = pd.cut(
        filtered["duration"],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True
    )

    # --- Enforce the category order (for consistent color mapping) ---
    filtered["duration_bin"] = pd.Categorical(
        filtered["duration_bin"],
        categories=labels,
        ordered=True
    )

    # --- Compute max H3 distance per bin from the fragmented data ---
    # Keep the order of labels as they appear in your cut (we used the same labels variable to create them)
    labels_order = filtered["duration_bin"].cat.categories.tolist()
    bin_to_k = max_k_per_bin(source_h3, filtered)

    print("\n=== Max H3 distance (k) per duration bin ===")
    for label, k in bin_to_k.items():
        print(f"{label}: {k}")

    # --- Reconstruct concentric rings from those ks ---
    recon_df = reconstruct_ring_labels(source_h3, labels_order, bin_to_k)

    recon_df["duration_bin"] = pd.Categorical(
        recon_df["duration_bin"],
        categories=labels,
        ordered=True
    )

    # --- Keep rings only inside Utah and attach geometry by joining the canonical grid ---
    # (we use utah_hex_gdf to provide the polygons)
    rings_gdf = utah_hex_gdf.merge(recon_df, on="dest_h3", how="inner")

    # --- The fragmented layer is your already-filtered, observed cells ---
    fragmented_gdf = filtered.copy()  # already merged with utah_hex_gdf and has geometry + duration_bin

    # --- Plot both: reconstructed rings (background) + fragmented (foreground) ---
    plot_iterative_rings(rings_gdf, fragmented_gdf, utah_boundary, places, source_h3, THRESHOLDS, labels)

if __name__ == "__main__":
    main()