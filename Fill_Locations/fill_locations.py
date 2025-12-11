import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import pickle
import h3
import random

# -------------------------
# Paths / config
# -------------------------

POP_FOLDER = Path("Synthetic Population")  # folder with your parquet files
OUTPUT_FOLDER = Path("Synthetic_Pop_Filled")
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

DRIVETIME_DUCKDB = "Data/utah_drivetimes.duckdb"
DRIVETIME_TABLE = "utah_drivetimes"

ACT_AMN_PATH = "Amenities/activity_amenities_dictionary.pkl"
HEX_AMN_PATH = "Amenities/hex_amenities_dictionary.pkl"

# 10-minute bins (example; adjust as desired)
THRESHOLDS = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]

# day / hour ordering for chronological sorting
DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
HOUR_ORDER = [f"{str(h).zfill(2)}-{str(h+1).zfill(2)}" for h in range(24)]

# -------------------------
# Helpers: data loading
# -------------------------

def load_act_amn_dict():
    with open(ACT_AMN_PATH, "rb") as f:
        return pickle.load(f)

def load_hex_amn_dict():
    with open(HEX_AMN_PATH, "rb") as f:
        return pickle.load(f)

# -------------------------
# Helpers: drivetime → rings
# -------------------------

def safe_h3_distance(src, dst):
    """Wrapper around h3.grid_distance that returns None on error."""
    try:
        return h3.grid_distance(src, dst)
    except Exception:
        return None

def compute_bin_labels(thresholds):
    bins = [0] + thresholds  # e.g. [0, 500, 1000, ...]
    labels = [f"{bins[i]}–{bins[i+1]}" for i in range(len(bins) - 1)]
    return bins, labels

def compute_bin_to_k_for_source(source_h3, con, thresholds):
    """
    For a given source hex, compute {duration_bin -> max H3 distance (k)}.
    Uses only hexes that appear in the drivetime table.
    """
    max_thr = max(thresholds)
    query = f"""
        SELECT dest_h3, duration
        FROM {DRIVETIME_TABLE}
        WHERE source_h3 = ? AND duration > 0 AND duration <= ?
    """
    df = con.execute(query, [source_h3, max_thr]).df()
    if df.empty:
        return {}, None, None  # no drivetime data for this source

    bins, labels = compute_bin_labels(thresholds)

    # Bin durations
    df["duration_bin"] = pd.cut(
        df["duration"],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True
    )

    # Compute max H3 distance per bin
    tmp = df[["dest_h3", "duration_bin"]].dropna(subset=["duration_bin"]).copy()
    tmp["h3dist"] = tmp["dest_h3"].apply(lambda d: safe_h3_distance(source_h3, d))
    tmp = tmp.dropna(subset=["h3dist"])

    grouped = tmp.groupby("duration_bin", observed=False)["h3dist"].max()
    grouped = grouped.dropna()

    bin_to_k = grouped.astype(int).to_dict()
    return bin_to_k, bins, labels

def lookup_duration(con, source_h3, dest_h3):
    """
    Look up drivetime from source_h3 to dest_h3.
    Returns float duration or np.nan if not found.
    """
    if source_h3 is None or dest_h3 is None:
        return np.nan
    dur = con.execute(
        f"SELECT duration FROM {DRIVETIME_TABLE} WHERE source_h3 = ? AND dest_h3 = ?",
        [source_h3, dest_h3]
    ).fetchone()
    if dur is None:
        return np.nan
    return float(dur[0])

# -------------------------
# Core: assign location using fragmented isochrone + amenities
# -------------------------

def assign_location_with_drivetimes(
    source_h3,
    act_type,
    con,
    thresholds,
    act_amn_dict,
    hex_amn_dict,
    utah_hex_set,
    rng,
    no_match_counter
):
    """
    Try to assign a new h3_act location for (source_h3, act_type).
    1. Compute bin_to_k from drivetime data.
    2. For each bin (in increasing drivetime), build ring using H3 disk.
    3. Within ring, look for hexes whose amenities can host act_type.
    4. If found, choose random hex from candidates.
    5. If none found in any bin, choose random hex from largest disk (fallback).
    Returns:
        chosen_hex (str or None),
        duration_sec (float or np.nan),
        updated no_match_counter (int)
    """
    # If we have no source, we cannot do anything sophisticated
    if source_h3 is None:
        return None, np.nan, no_match_counter

    # Compute k per bin for this source
    bin_to_k, bins, labels = compute_bin_to_k_for_source(source_h3, con, thresholds)
    if not bin_to_k:
        # No drivetime info for this source hex: fallback later
        bins = [0] + thresholds
        labels = [f"{bins[i]}–{bins[i+1]}" for i in range(len(bins) - 1)]

    activity_amenities = act_amn_dict.get(act_type, [])
    assigned = set()
    chosen_hex = None
    duration_sec = np.nan
    found_match = False

    # 1) Try each drivetime bin in increasing order
    for label in labels:
        k = bin_to_k.get(label)
        if not k or k <= 0:
            continue

        disk = set(h3.grid_disk(source_h3, int(k)))
        ring = disk - assigned
        assigned |= disk

        # Optionally restrict to Utah + only hexes we know about in hex_amn_dict
        ring = [h for h in ring if (utah_hex_set is None or h in utah_hex_set)]

        # Filter by amenities that can host this activity
        candidates = []
        for hx in ring:
            amenities = hex_amn_dict.get(hx, [])
            if any(a in amenities for a in activity_amenities):
                candidates.append(hx)

        if candidates:
            chosen_hex = rng.choice(candidates)
            found_match = True
            break

    # 2) Fallback if *no* amenity-matching hex was found
    if not found_match:
        no_match_counter += 1
        if bin_to_k:
            # Use largest disk
            max_k = max(bin_to_k.values())
            disk = set(h3.grid_disk(source_h3, int(max_k)))
            disk = [h for h in disk if (utah_hex_set is None or h in utah_hex_set)]
            if disk:
                chosen_hex = rng.choice(disk)
        # If still none, chosen_hex stays None

    # 3) Look up drivetime if we have a chosen hex
    if chosen_hex is not None:
        duration_sec = lookup_duration(con, source_h3, chosen_hex)

    return chosen_hex, duration_sec, no_match_counter

# -------------------------
# Processing one parquet file
# -------------------------

def process_population_file(
    file_path,
    con,
    thresholds,
    act_amn_dict,
    hex_amn_dict,
    utah_hex_set,
    available_sources_set,
    rng
):
    """
    Reads one parquet file, fills h3_act where missing using drivetime-informed rings, and adds a 'drivetime_sec' column.
    Returns updated DataFrame and counts:
        no_match_counter: times we exhausted all rings with no amenity match
        legacy_fallback_counter: times we had to use local small-disk method
    """
    df = pd.read_parquet(file_path)

    # Create new columns
    df["h3_act"] = df["h3_act"].astype("object")  # ensure we can assign strings / None
    df["drivetime_sec"] = np.nan

    # Add sort keys for chronological order
    day_index = {d: i for i, d in enumerate(DAY_ORDER)}
    hour_index = {h: i for i, h in enumerate(HOUR_ORDER)}

    df["day_order"] = df["day"].map(day_index)
    df["hour_order"] = df["hour"].map(hour_index)

    df = df.sort_values(["p_id", "day_order", "hour_order"])

    no_match_counter = 0
    legacy_fallback_counter = 0   

    # Iterate per agent
    for pid, group_idx in df.groupby("p_id", observed=False).groups.items():
        idxs = list(group_idx)
        prev_act = None
        prev_hex = None

        # We can read home hex once for this agent
        home_hex = df.loc[idxs[0], "h3_home"]

        for i, row_idx in enumerate(idxs):
            row = df.loc[row_idx]
            act_type = row["act"]
            current_hex = row["h3_act"]

            # If same activity as previous and we already had a location, stay put
            if act_type == prev_act and prev_hex is not None:
                df.at[row_idx, "h3_act"] = prev_hex
                df.at[row_idx, "drivetime_sec"] = 0.0
                continue

            # Determine source hex for this step
            source_hex = prev_hex if prev_hex is not None else home_hex

            # Case 1: activity already has a location
            if pd.notna(current_hex):
                df.at[row_idx, "h3_act"] = current_hex
                # Compute drivetime from source_hex to current_hex if we can
                if source_hex is not None and source_hex in available_sources_set:
                    dur = lookup_duration(con, source_hex, current_hex)
                    if not np.isnan(dur):
                        df.at[row_idx, "drivetime_sec"] = dur
                prev_hex = current_hex

            # Case 2: h3_act is missing → we need to assign
            else:
                chosen_hex = None
                duration_sec = np.nan

                if source_hex is not None and source_hex in available_sources_set:
                    # Use drivetime-informed rings
                    chosen_hex, duration_sec, no_match_counter = assign_location_with_drivetimes(
                        source_hex,
                        act_type,
                        con,
                        thresholds,
                        act_amn_dict,
                        hex_amn_dict,
                        utah_hex_set,
                        rng,
                        no_match_counter
                    )
                else:
                    # Fallback: small local disk around source_hex using amenities
                    if source_hex is not None:
                        legacy_fallback_counter += 1  
                        disk_size = 3
                        neighbors = h3.grid_disk(source_hex, disk_size)
                        # restrict to Utah and amenity-eligible
                        activity_amenities = act_amn_dict.get(act_type, [])
                        candidates = []
                        for hx in neighbors:
                            if utah_hex_set is not None and hx not in utah_hex_set:
                                continue
                            amenities = hex_amn_dict.get(hx, [])
                            if any(a in amenities for a in activity_amenities):
                                candidates.append(hx)
                        if candidates:
                            chosen_hex = rng.choice(candidates)
                        else:
                            # even here: fallback to any neighbor
                            chosen_hex = rng.choice(list(neighbors))
                    # duration will remain NaN (no drivetime info used)

                # Final fallback: if still no hex, stay at home_hex
                if chosen_hex is None:
                    chosen_hex = source_hex if source_hex is not None else home_hex

                df.at[row_idx, "h3_act"] = chosen_hex
                if not np.isnan(duration_sec):
                    df.at[row_idx, "drivetime_sec"] = duration_sec

                prev_hex = chosen_hex

            prev_act = act_type

        print(f"Finished with agent: {pid}")

    # Clean up helper columns
    df = df.drop(columns=["day_order", "hour_order"])

    return df, no_match_counter, legacy_fallback_counter

# -------------------------
# Main driver
# -------------------------

def main():
    act_amn_dict = load_act_amn_dict()
    hex_amn_dict = load_hex_amn_dict()

    # Optionally: use all hexes that appear in amenities dict as "Utah hex set"
    utah_hex_set = set(hex_amn_dict.keys())

    # RNG so things are reproducible if desired
    rng = random.Random(123)

    # Connect to DuckDB and cache available sources
    con = duckdb.connect(DRIVETIME_DUCKDB, read_only=True)
    src_rows = con.execute(f"SELECT DISTINCT source_h3 FROM {DRIVETIME_TABLE}").fetchall()
    available_sources_set = {r[0] for r in src_rows}

    global_no_match = 0
    global_legacy_fallback = 0  

    # --- Only process first 3 files for debugging ---
    files = sorted(POP_FOLDER.glob("*.parquet"))[:3]  

    for file_path in files:
        print(f"Processing {file_path.name} ...")
        updated_df, no_match, legacy_fallback = process_population_file(
            file_path=file_path,
            con=con,
            thresholds=THRESHOLDS,
            act_amn_dict=act_amn_dict,
            hex_amn_dict=hex_amn_dict,
            utah_hex_set=utah_hex_set,
            available_sources_set=available_sources_set,
            rng=rng
        )
        global_no_match += no_match
        global_legacy_fallback += legacy_fallback

        out_path = OUTPUT_FOLDER / file_path.name
        updated_df.to_parquet(out_path, index=False)
        print(f"  Saved updated file to {out_path}")
        print(f"  No-match-after-all-rings cases in this file: {no_match}")
        print(f"  Legacy small-disk fallback uses in this file: {legacy_fallback}")

    print("\n=== Summary across processed files ===")
    print(f"Total 'no amenity match after all rings' cases: {global_no_match}")
    print(f"Total 'legacy small-disk fallback' uses:       {global_legacy_fallback}")

if __name__ == "__main__":
    main()