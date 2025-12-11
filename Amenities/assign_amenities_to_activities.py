import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from pathlib import Path
import pickle


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Determine the set of unique amenities in Maksudul's data
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# Folder containing your amenity CSV files
folder_path = Path("/Users/p5d/Documents/Python/ABM Practice/Utah/AmenitiesData") # an enormous file local to my machine, this program makes it so that users don't need the original files to run our simulation

# Set to store all unique amenities
unique_amenities = set()

# Loop through all CSV files in the folder
for file in folder_path.glob("*.csv"):
    df = pd.read_csv(file)

    # Add non-null amenities to the set
    unique_amenities.update(df["amenity"].dropna().unique())

# Print the results
# unique_amenities_list = sorted(unique_amenities)
# print(f"Total unique amenities found: {len(unique_amenities_list)}")
# for amenity in unique_amenities_list:
#     print(amenity)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Match as many amenities as we can using Joe's source code
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# Convert to set of "key=value" format
unique_amenities = set(map(str.strip, unique_amenities))

# Joe's original source mapping
activity_source_map = {
    "home": [],
    "work_home": [],
    "work_emp": [],
    "work_related": [
        "weighbridge",
        "sanitary_dump_station",
        "office=advertising_agency",
        "office=coworking",
        "office=employment_agency",
        "gas"
    ],
    "volunteer": [
        "animal_shelter",
        "office=association",
        "office=charity",
        "office=political_party",
        "charity"
    ],
    "dropoff_pickup": [],
    "change_transp": [
        "bicycle_parking",
        "bus_station",
        "car_rental",
        "ferry_terminal",
        "grit_bin",
        "motorcycle_parking",
        "parking",
        "parking_entrance",
        "parking_space",
        "taxi",
        "public_transport=stop_position",
        "public_transport=platform",
        "public_transport=station",
        "public_transport=stop_area",
        "public_transport=stop_area_group"
    ],
    "school": [
        "college",
        "driving_school",
        "first_aid_school",
        "kindergarten",
        "language_school",
        "research_institute",
        "training",
        "music_school",
        "school",
        "traffic_park",
        "university",
        "driver_training"
    ],
    "childcare": [],
    "adult_care": [
        "nursing_home"
    ],
    "retail": [
        "charging_station",
        "fuel",
        "marketplace",
        "vending_machine",
        "alcohol",
        "bakery",
        "beverages",
        "brewing_supplies",
        "butcher",
        "cheese",
        "chocolate",
        "coffee",
        "confectionery",
        "convenience",
        "dairy",
        "deli",
        "farm",
        "food",
        "frozen_food",
        "greengrocer",
        "health_food",
        "ice_cream",
        "nuts",
        "pasta",
        "pastry",
        "seafood",
        "spices",
        "tea",
        "tortilla",
        "water",
        "wine",
        "department_store",
        "general",
        "kiosk",
        "mall",
        "supermarket",
        "wholesale",
        "baby_goods",
        "bag",
        "boutique",
        "clothes",
        "fabric",
        "fashion",
        "fashion_accessories",
        "jewelry",
        "leather",
        "sewing",
        "shoes",
        "shoe_repair",
        "tailor",
        "watches",
        "wool",
        "charity",
        "second_hand",
        "variety_store",
        "beauty",
        "chemist",
        "cosmetics",
        "erotic",
        "hairdresser",
        "hairdresser_supply",
        "hearing_aids",
        "herbalist",
        "massage",
        "medical_supply",
        "nutrition_supplements",
        "optician",
        "perfumery",
        "agrarian",
        "appliance",
        "bathroom_furnishing",
        "country_store",
        "doityourself",
        "electrical",
        "energy",
        "fireplace",
        "florist",
        "garden_centre",
        "garden_furniture",
        "groundskeeping",
        "hardware",
        "houseware",
        "locksmith",
        "paint",
        "pottery",
        "security",
        "tool_hire",
        "trade",
        "antiques",
        "bed",
        "candles",
        "carpet",
        "curtain",
        "doors",
        "flooring",
        "furniture",
        "household_linen",
        "interior_decoration",
        "kitchen",
        "lighting",
        "tiles",
        "window_blind",
        "computer",
        "electronics",
        "hifi",
        "mobile_phone",
        "printer_ink",
        "radiotechnics",
        "telecommunication",
        "vacuum_cleaner",
        "atv",
        "bicycle",
        "boat",
        "car",
        "car_parts",
        "caravan",
        "fishing",
        "fuel",
        "golf",
        "hunting",
        "military_surplus",
        "motorcycle",
        "motorcycle_repair",
        "outdoor",
        "scuba_diving",
        "ski",
        "snowmobile",
        "sports",
        "surf",
        "swimming_pool",
        "trailer",
        "truck",
        "tyres",
        "art",
        "camera",
        "collector",
        "craft",
        "frame",
        "games",
        "model",
        "music",
        "musical_instrument",
        "photo",
        "trophy",
        "video",
        "video_games",
        "anime",
        "books",
        "gift",
        "lottery",
        "newsagent",
        "stationery",
        "ticket",
        "cannabis",
        "dry_cleaning",
        "e-cigarette",
        "outpost",
        "party",
        "pawnbroker",
        "pest_control",
        "pet",
        "pyrotechnics",
        "religion",
        "rental",
        "tobacco",
        "toys",
        "weapons",
        "yes",
        "user defined"
    ],
    "services": [
        "bicycle_repair_station",
        "car_sharing",
        "car_wash",
        "compressed_air",
        "vehicle_inspection",
        "atm",
        "payment_terminal",
        "bank",
        "bureau_de_change",
        "money_transfer",
        "payment_centre",
        "veterinary",
        "telephone",
        "animal_boarding",
        "animal_training",
        "office=accountant",
        "office=architect",
        "office=consulting",
        "office=event_management",
        "office=financial",
        "office=financial_advisor",
        "office=graphic_design",
        "office=insurance",
        "office=lawyer",
        "office=notary",
        "office=tax_advisor",
        "office=travel_agent",
        "office=tutoring",
        "tattoo",
        "glaziery",
        "car_repair",
        "bookmaker",
        "copyshop",
        "funeral_directors",
        "laundry",
        "money_lender",
        "pet_grooming",
        "storage_rental",
        "travel_agency"
    ],
    "meals": [
        "bar",
        "biergarten",
        "cafe",
        "fast_food",
        "food_court",
        "ice_cream",
        "pub",
        "restaurant",
        "baking_oven",
        "kitchen",
        "coffee"
    ],
    "errands": [
        "library",
        "toy_library",
        "bicycle_wash",
        "pharmacy",
        "courthouse",
        "post_box",
        "post_depot",
        "townhall",
        "mailroom",
        "parcel_locker",
        "recycling",
        "waste_disposal",
        "waste_transfer_station",
        "office=visa"
    ],
    "leisure": [
        "bicycle_rental",
        "boat_rental",
        "boat_sharing",
        "arts_centre",
        "brothel",
        "casino",
        "cinema",
        "community_centre",
        "conference_centre",
        "events_venue",
        "exhibition_centre",
        "fountain",
        "gambling",
        "love_hotel",
        "music_venue",
        "nightclub",
        "planetarium",
        "public_bookcase",
        "social_centre",
        "stage",
        "stripclub",
        "swingerclub",
        "theatre",
        "ranger_station",
        "bbq",
        "bench",
        "dog_toilet",
        "dressing_room",
        "drinking_water",
        "lounge",
        "shelter",
        "shower",
        "watering_place",
        "dive_centre",
        "hunting_stand",
        "internet_cafe",
        "kneipp_water_cure",
        "lounger",
        "photo_booth",
        "public_bath",
        "historic=aircraft",
        "historic=anchor",
        "historic=aqueduct",
        "historic=archaeological_site",
        "historic=battlefield",
        "historic=bomb_crater",
        "historic=boundary_stone",
        "historic=building",
        "historic=bullaun_stone",
        "historic=cannon",
        "historic=castle",
        "historic=castle_wall",
        "historic=cattle_crush",
        "historic=charcoal_pile",
        "historic=city_gate",
        "historic=citywalls",
        "historic=creamery",
        "historic=district",
        "historic=epigraph",
        "historic=farm",
        "historic=fort",
        "historic=gallows",
        "historic=house",
        "historic=high_cross",
        "historic=highwater_mark",
        "historic=lavoir",
        "historic=lime_kiln",
        "historic=locomotive",
        "historic=machine",
        "historic=manor",
        "historic=memorial",
        "historic=milestone",
        "historic=millstone",
        "historic=mine",
        "historic=minecart",
        "historic=monument",
        "historic=ogham_stone",
        "historic=optical_telegraph",
        "historic=pa",
        "historic=pillory",
        "historic=pound",
        "historic=railway_car",
        "historic=road",
        "historic=round_tower",
        "historic=ruins",
        "historic=rune_stone",
        "historic=shieling",
        "historic=ship",
        "historic=ste\u0107ak",
        "historic=stone",
        "historic=tank",
        "historic=tomb",
        "historic=tower",
        "historic=vehicle",
        "historic=wreck",
        "historic=yes",
        "leisure=adult_gaming_centre",
        "leisure=amusement_arcade",
        "leisure=beach_resort",
        "leisure=bandstand",
        "leisure=bird_hide",
        "leisure=common",
        "leisure=dance",
        "leisure=disc_golf_course",
        "leisure=dog_park",
        "leisure=escape_game",
        "leisure=firepit",
        "leisure=fishing",
        "leisure=garden",
        "leisure=hackerspace",
        "leisure=ice_rink",
        "leisure=marina",
        "leisure=miniature_golf",
        "leisure=nature_reserve",
        "leisure=park",
        "leisure=picnic_table",
        "leisure=playground",
        "leisure=slipway",
        "leisure=sports_centre",
        "leisure=stadium",
        "leisure=summer_camp",
        "leisure=swimming_area",
        "leisure=swimming_pool",
        "office=guide",
        "alpine_hut",
        "aquarium",
        "artwork",
        "attraction",
        "camp_pitch",
        "camp_site",
        "caravan_site",
        "gallery",
        "information",
        "museum",
        "picnic_site",
        "theme_park",
        "viewpoint",
        "wilderness_hut",
        "zoo",
        "yes"
    ],
    "exercise": [
        "dancing_school",
        "surf_school",
        "leisure=dance",
        "leisure=disc_golf_course",
        "leisure=fitness_centre",
        "leisure=fitness_station",
        "leisure=horse_riding",
        "leisure=ice_rink",
        "leisure=nature_reserve",
        "leisure=park",
        "leisure=pitch",
        "leisure=sports_centre",
        "leisure=swimming_area",
        "leisure=swimming_pool",
        "leisure=track"
    ],
    "visit_friends_relatives": [],
    "medical": [
        "clinic",
        "dentist",
        "doctors",
        "hospital",
        "healthcare=alternative",
        "healthcare=audiologist",
        "healthcare=birthing_centre",
        "healthcare=blood_bank",
        "healthcare=blood_donation",
        "healthcare=counselling",
        "healthcare=dialysis",
        "healthcare=hospice",
        "healthcare=laboratory",
        "healthcare=midwife",
        "healthcare=nurse",
        "healthcare=occupational_therapist",
        "healthcare=optometrist",
        "healthcare=physiotherapist",
        "healthcare=podiatrist",
        "healthcare=psychotherapist",
        "healthcare=rehabilitation",
        "healthcare=sample_collection",
        "healthcare=speech_therapist",
        "healthcare=vaccination_centre"
    ],
    "religious_community": [
        "social_facility",
        "monastery",
        "place_of_worship",
        "historic=church",
        "historic=monastery",
        "historic=mosque",
        "historic=temple",
        "historic=wayside_cross",
        "historic=wayside_shrine",
        "office=association",
        "office=charity",
        "office=political_party"
    ],
    "other": [
        "baby_hatch",
        "studio",
        "fire_station",
        "police",
        "post_office",
        "prison",
        "give_box",
        "toilets",
        "water_point",
        "waste_basket",
        "animal_breeding",
        "clock",
        "crematorium",
        "funeral_hall",
        "grave_yard",
        "mortuary",
        "place_of_mourning",
        "public_building",
        "refugee_site",
        "user defined",
        "office=estate_agent"
    ]
}

# Full list of activities
ACTIVITIES = [
    "home", "dropoff_pickup", "childcare", "adult_care",
    "work_emp", "work_home", "work_related", "volunteer",
    "school", "change_transp", "retail", "meals",
    "errands", "services", "leisure", "exercise", 
    "visit_friends_relatives", "medical", "religious_community", "other"
]

# New cleaned mapping: keep only observed amenities
filtered_activity_map = {}

for activity in ACTIVITIES:
    source_amenities = activity_source_map.get(activity, [])
    # Keep only amenities that were actually observed in your data
    matched = [a for a in source_amenities if a.split("=")[-1] in unique_amenities]
    filtered_activity_map[activity] = matched

# Print or save result
# for activity, amenities in filtered_activity_map.items():
#     print(f"{activity}: {len(amenities)} amenities")
#     for a in amenities:
#         print(f"  - {a}")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Determine which amenities where not matched to any activity
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# Step 1: Collect all amenities that were matched in filtered_activity_map
matched_amenities = set()
for amenities in filtered_activity_map.values():
    matched_amenities.update([a.split("=")[-1] for a in amenities])

# Step 2: Compute unmatched amenities
unmatched_amenities = unique_amenities - matched_amenities

# Step 3: Print them
# print(f"\nUnmatched amenities ({len(unmatched_amenities)}):")
# for amenity in sorted(unmatched_amenities):
#     print(f"  - {amenity}")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Final dictionary assigning each activity a list of amenities
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
final_activity_amenities = {
    "home": [],
    "work_emp": [],
    "work_home": [],
    "travel": [],
    "unknown": [],
    "dropoff_pickup": [
        "childcare", "library_dropoff"
    ],
    "childcare": [
        "childcare"
    ],
    "adult_care": [
        "nursing_home"
    ],
    "work_related": [
        "weighbridge", "sanitary_dump_station", "coworking_space"
    ],
    "volunteer": [
        "animal_shelter", "shelter"
    ],
    "school": [
        "college", "driving_school", "kindergarten",
        "research_institute", "training", "music_school",
        "school", "university", "driver_training",
        "flight_school", "art_school", "hairdressing_school"
    ],
    "change_transp": [
        "bicycle_parking", "bus_station", "car_rental",
        "motorcycle_parking", "parking", "parking_entrance",
        "parking_space", "taxi", "ticket_validator"
    ],
    "retail": [
        "charging_station", "fuel", "marketplace",
        "vending_machine", "ice_cream", "vacuum_cleaner",
        "grocery", "trolley_bay"
    ],
    "meals": [
        "bar", "biergarten", "cafe", "fast_food",
        "food_court", "ice_cream", "pub", "restaurant"
    ],
    "errands": [
        "library", "pharmacy", "courthouse", "post_box",
        "post_depot", "townhall", "mailroom",
        "parcel_locker", "recycling", "waste_disposal",
        "waste_transfer_station", "polling_station", "post_office"
    ],
    "services": [
        "bicycle_repair_station", "car_wash", "compressed_air",
        "vehicle_inspection", "atm", "payment_terminal",
        "bank", "bureau_de_change", "money_transfer",
        "payment_centre", "veterinary", "telephone",
        "animal_boarding", "animal_training", "money_lender",
        "hairdressing_school", "spa", "storage", "kennel", "teeth_whitening",
        "post_office"
    ],
    "leisure": [
        "bicycle_rental", "boat_rental", "boat_sharing",
        "arts_centre", "casino", "cinema", "community_centre",
        "conference_centre", "events_venue", "exhibition_centre",
        "fountain", "music_venue", "nightclub", "planetarium",
        "public_bookcase", "social_centre", "stage", "stripclub",
        "theatre", "ranger_station", "bbq", "bench",
        "dive_centre", "internet_cafe", "lounger", "public_bath",
        "bar", "spa", "art_school", "skateboard_parking", "studio"
    ],
    "exercise": [
        "dancing_school", "dojos", "parcourse", "skateboard_parking"
    ],
    "visit_friends_relatives": [
        "social_facility", "place_of_worship", "hospital",
        "nursing_home", "college", "university", "cafe",
        "bar", "biergarten", "pub", "restaurant"
    ],
    "medical": [
        "clinic", "dentist", "doctors", "hospital",
        "teeth_whitening"
    ],
    "religious_community": [
        "social_facility", "place_of_worship"
    ],
    "other": [ # prison, 
        "fire_station", "police",
        "give_box", "toilets", "water_point",
        "waste_basket", "animal_breeding", "clock",
        "crematorium", "grave_yard", "mortuary",
        "public_building", "dog_toilet", "drinking_water",
        "shower", "watering_place", "information", "dressing_room"
    ]
}

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Save the final dictionary of amenities as a pickle file
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
with open("Amenities/activity_amenities_dictionary.pkl", "wb") as f:
    pickle.dump(final_activity_amenities, f)
