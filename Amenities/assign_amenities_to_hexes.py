import os
import pandas as pd
import pickle
from pathlib import Path

def build_hex_amenity_lookup(csv_folder):
    hex_amenity_dict = {}

    for csv_file in Path(csv_folder).glob("*.csv"):
        hex_id = csv_file.stem  # get filename without .csv
        try:
            df = pd.read_csv(csv_file)
            if "amenity" in df.columns:
                amenities = df["amenity"].dropna().unique().tolist()
                hex_amenity_dict[hex_id] = amenities
        except Exception as e:
            print(f"Failed to process {csv_file}: {e}")
            hex_amenity_dict[hex_id] = []

    return hex_amenity_dict

if __name__ == "__main__":
    folder_path = "/Users/p5d/Documents/Python/ABM Practice/Utah/AmenitiesData" # an enormous file local to my machine, this program makes it so that users don't need the original files to run our simulation
    lookup = build_hex_amenity_lookup(folder_path)

    with open("Amenities/hex_amenities_dictionary.pkl", "wb") as f:
        pickle.dump(lookup, f)

    print(f"Built and saved amenity lookup for {len(lookup)} hexes.")
