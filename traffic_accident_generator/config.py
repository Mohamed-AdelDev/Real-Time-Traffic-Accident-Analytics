"""
config.py
=========
Central settings and lookup tables for the SYNTHETIC Traffic Accident Dataset Generator.

IMPORTANT: every record produced by this project is SYNTHETIC (computer generated).
It is NOT a real accident report and must never be presented as real-world observation.
Road names, areas and coordinates are only *plausible* labels used for testing a
Big Data pipeline (Kafka -> NiFi -> Spark -> PostgreSQL -> Power BI / Streamlit).

Beginner tip
------------
* Section 1 holds the settings you will most often change (dataset size, seed, dates).
* Everything after that is a "realism knob": weights and probabilities that make the
  data look like real traffic statistics. You normally do not need to touch them.
* A "weights" dictionary such as {"Car": 62, "Van": 9} means "pick Car 62 times out of
  71, Van 9 times out of 71". The weights do NOT have to add up to 100.
"""
from datetime import date

# =====================================================================
# 1. MAIN SETTINGS  (can also be overridden from the command line)
# =====================================================================
NUM_ACCIDENTS = 100_000            # use 10_000 for quick test runs
RANDOM_SEED = 42                   # same seed + same settings = identical dataset
START_DATE = date(2023, 1, 1)      # timeline start (inclusive)
END_DATE = date(2024, 12, 31)      # timeline end (inclusive) -> two full years
OUTPUT_DIR = "traffic_accident_dataset"
CHUNK_SIZE = 10_000                # accidents processed per batch (keeps memory low)
COUNTRY = "United Kingdom"

# =====================================================================
# 2. TIME DISTRIBUTIONS
# =====================================================================
# Relative accident volume per calendar month (Jan..Dec): darker autumn/winter
# months are busier, February is the quietest.
MONTH_WEIGHTS = [0.95, 0.90, 0.95, 0.97, 1.02, 1.03, 1.05, 1.02, 1.05, 1.12, 1.12, 1.05]

# Relative accident volume per weekday (Monday..Sunday). Friday is the busiest.
DAY_OF_WEEK_WEIGHTS = [0.98, 0.98, 1.00, 1.02, 1.18, 1.08, 0.88]

# Small yearly trend: each year later has 3% fewer accidents (gives a visible trend line).
YEARLY_TREND = -0.03

# Relative accident volume per hour (0..23).
# Weekdays: two clear rush-hour peaks (07-09 and 16-18).
HOUR_WEIGHTS_WEEKDAY = [1.2, 0.8, 0.6, 0.5, 0.5, 0.9, 2.2, 4.8, 7.0, 5.0, 3.8, 3.8,
                        4.4, 4.3, 4.5, 5.6, 7.0, 8.2, 6.0, 3.9, 3.0, 2.5, 2.1, 1.6]
# Weekends: no rush hours, a broad afternoon plateau and a late-night bump.
HOUR_WEIGHTS_WEEKEND = [3.2, 3.0, 2.6, 1.8, 1.2, 1.0, 1.3, 1.8, 2.6, 3.6, 4.8, 5.6,
                        6.0, 6.2, 6.3, 6.2, 6.0, 5.6, 4.8, 4.2, 3.9, 3.8, 3.8, 3.6]

# =====================================================================
# 3. GEOGRAPHY  (10 UK cities, each with areas and 2 accident hotspots)
# =====================================================================
URBAN_SHARE = 0.72            # share of accidents that happen in the built-up city area
HOTSPOT_SHARE = 0.12          # share of URBAN accidents that fall on a hotspot junction
HOTSPOT_SPREAD_DEG = 0.0012   # ~130 m: hotspot accidents are tightly clustered
AREA_SPREAD_LAT = 0.010       # ~1.1 km spread around an area centre
AREA_SPREAD_LON = 0.015
RURAL_MIN_KM = 10             # rural accidents happen 10-24 km from the city centre
RURAL_MAX_KM = 24
BBOX_HALF_LAT = 0.25          # every coordinate stays inside city centre +/- this box
BBOX_HALF_LON = 0.40

# Each area is (name, latitude, longitude, weight). Weights decide how busy the area is.
# "climate" is (rain multiplier, snow multiplier, temperature adjustment in C).
# "rural_bearing" (compass degrees) keeps rural points away from the sea where relevant.
CITIES = [
    {"name": "London", "weight": 24, "center": (51.5074, -0.1278),
     "climate": (0.85, 0.5, 1.2),
     "areas": [("Westminster", 51.4975, -0.1357, 1.3), ("Camden", 51.5390, -0.1426, 1.1),
               ("Hackney", 51.5450, -0.0553, 1.0), ("Croydon", 51.3762, -0.0982, 1.0),
               ("Greenwich", 51.4826, 0.0077, 0.9), ("Islington", 51.5362, -0.1033, 1.0)],
     "motorways": ["M25", "M4", "M1"],
     "a_roads": ["A40", "A406", "A12", "A3", "A23", "A10"],
     "hotspots": [
         {"area": "Westminster", "dlat": 0.003, "dlon": -0.004, "road_name": "A3212 Embankment",
          "road_type": "A Road", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "Croydon", "dlat": -0.004, "dlon": 0.006, "road_name": "A23 Purley Way",
          "road_type": "Dual Carriageway", "junction_type": "Roundabout", "weight": 1.0}]},

    {"name": "Birmingham", "weight": 11, "center": (52.4862, -1.8904),
     "climate": (1.0, 0.9, 0.0),
     "areas": [("City Centre", 52.4862, -1.8904, 1.5), ("Edgbaston", 52.4584, -1.9301, 0.9),
               ("Digbeth", 52.4750, -1.8810, 0.9), ("Aston", 52.5049, -1.8820, 1.0),
               ("Selly Oak", 52.4417, -1.9358, 0.8), ("Handsworth", 52.5090, -1.9250, 0.8)],
     "motorways": ["M6", "M5", "M42"],
     "a_roads": ["A38", "A45", "A34", "A41", "A4040"],
     "hotspots": [
         {"area": "Aston", "dlat": 0.004, "dlon": 0.003, "road_name": "A38(M) Aston Expressway",
          "road_type": "Dual Carriageway", "junction_type": "Slip Road", "weight": 1.0},
         {"area": "Digbeth", "dlat": -0.003, "dlon": 0.004, "road_name": "A4540 Ring Road",
          "road_type": "Dual Carriageway", "junction_type": "Roundabout", "weight": 1.0}]},

    {"name": "Manchester", "weight": 11, "center": (53.4808, -2.2426),
     "climate": (1.25, 0.8, -0.2),
     "areas": [("City Centre", 53.4808, -2.2426, 1.6), ("Salford", 53.4875, -2.2901, 1.0),
               ("Didsbury", 53.4167, -2.2314, 0.7), ("Stretford", 53.4458, -2.3077, 0.9),
               ("Moss Side", 53.4527, -2.2452, 0.9), ("Longsight", 53.4607, -2.2022, 0.8)],
     "motorways": ["M60", "M62", "M56"],
     "a_roads": ["A56", "A6", "A57", "A34", "A5103"],
     "hotspots": [
         {"area": "City Centre", "dlat": 0.004, "dlon": 0.003, "road_name": "A57(M) Mancunian Way",
          "road_type": "Dual Carriageway", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "Stretford", "dlat": -0.003, "dlon": -0.004, "road_name": "A56 Chester Road",
          "road_type": "A Road", "junction_type": "Crossroads", "weight": 1.0}]},

    {"name": "Leeds", "weight": 8, "center": (53.8008, -1.5491),
     "climate": (1.05, 1.0, -0.5),
     "areas": [("City Centre", 53.8008, -1.5491, 1.5), ("Headingley", 53.8190, -1.5810, 1.0),
               ("Armley", 53.7960, -1.6040, 0.9), ("Chapel Allerton", 53.8310, -1.5340, 0.8),
               ("Hunslet", 53.7800, -1.5290, 0.8)],
     "motorways": ["M1", "M62", "M621"],
     "a_roads": ["A64", "A65", "A58", "A61", "A660"],
     "hotspots": [
         {"area": "City Centre", "dlat": -0.004, "dlon": 0.003, "road_name": "A58(M) Inner Ring Road",
          "road_type": "Dual Carriageway", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "Headingley", "dlat": 0.003, "dlon": -0.003, "road_name": "A660 Otley Road",
          "road_type": "A Road", "junction_type": "Crossroads", "weight": 1.0}]},

    {"name": "Glasgow", "weight": 9, "center": (55.8642, -4.2518),
     "climate": (1.35, 1.2, -0.8),
     "areas": [("City Centre", 55.8642, -4.2518, 1.5), ("Partick", 55.8700, -4.3090, 0.9),
               ("Govan", 55.8620, -4.3160, 1.0), ("Shettleston", 55.8570, -4.1700, 0.8),
               ("Pollokshaws", 55.8270, -4.2980, 0.8)],
     "motorways": ["M8", "M74", "M77"],
     "a_roads": ["A77", "A82", "A8", "A814", "A736"],
     "hotspots": [
         {"area": "Govan", "dlat": 0.003, "dlon": 0.004, "road_name": "A8 Govan Road",
          "road_type": "A Road", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "City Centre", "dlat": 0.003, "dlon": -0.003, "road_name": "M8 Junction 18 Approach",
          "road_type": "Dual Carriageway", "junction_type": "Slip Road", "weight": 1.0}]},

    {"name": "Liverpool", "weight": 8, "center": (53.4084, -2.9916),
     "climate": (1.15, 0.7, 0.2), "rural_bearing": (20, 200),
     "areas": [("City Centre", 53.4084, -2.9916, 1.5), ("Anfield", 53.4308, -2.9609, 0.9),
               ("Toxteth", 53.3880, -2.9660, 0.9), ("Everton", 53.4276, -2.9718, 0.9),
               ("Wavertree", 53.4010, -2.9437, 0.8)],
     "motorways": ["M62", "M57", "M58"],
     "a_roads": ["A580", "A59", "A561", "A5036"],
     "hotspots": [
         {"area": "Wavertree", "dlat": 0.003, "dlon": 0.004, "road_name": "A5047 Edge Lane",
          "road_type": "A Road", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "Toxteth", "dlat": -0.003, "dlon": 0.002, "road_name": "A561 Aigburth Road",
          "road_type": "A Road", "junction_type": "Crossroads", "weight": 1.0}]},

    {"name": "Nottingham", "weight": 8, "center": (52.9548, -1.1581),
     "climate": (0.9, 0.9, 0.0),
     "areas": [("City Centre", 52.9548, -1.1581, 1.5), ("Lenton", 52.9462, -1.1830, 0.9),
               ("Beeston", 52.9270, -1.2150, 0.8), ("West Bridgford", 52.9320, -1.1330, 0.9),
               ("Sherwood", 52.9840, -1.1450, 0.8)],
     "motorways": ["M1"],
     "a_roads": ["A52", "A453", "A60", "A610", "A6005"],
     "hotspots": [
         {"area": "Lenton", "dlat": 0.003, "dlon": 0.003, "road_name": "A52 Queens Drive",
          "road_type": "Dual Carriageway", "junction_type": "Roundabout", "weight": 1.0},
         {"area": "West Bridgford", "dlat": -0.003, "dlon": 0.004, "road_name": "A60 Loughborough Road",
          "road_type": "A Road", "junction_type": "Crossroads", "weight": 1.0}]},

    {"name": "Bristol", "weight": 7, "center": (51.4545, -2.5879),
     "climate": (1.0, 0.6, 0.8), "rural_bearing": (20, 200),
     "areas": [("City Centre", 51.4545, -2.5879, 1.5), ("Clifton", 51.4545, -2.6190, 0.9),
               ("Bedminster", 51.4400, -2.6020, 0.9), ("Redland", 51.4680, -2.6000, 0.8),
               ("Easton", 51.4620, -2.5600, 0.9)],
     "motorways": ["M32", "M5", "M4"],
     "a_roads": ["A38", "A4", "A420", "A37", "A4018"],
     "hotspots": [
         {"area": "City Centre", "dlat": 0.003, "dlon": 0.004, "road_name": "A4044 Temple Way",
          "road_type": "Dual Carriageway", "junction_type": "Roundabout", "weight": 1.0},
         {"area": "Bedminster", "dlat": -0.003, "dlon": 0.002, "road_name": "A38 Bedminster Down Road",
          "road_type": "A Road", "junction_type": "T-Junction", "weight": 1.0}]},

    {"name": "Sheffield", "weight": 7, "center": (53.3811, -1.4701),
     "climate": (1.05, 1.3, -0.7),
     "areas": [("City Centre", 53.3811, -1.4701, 1.5), ("Hillsborough", 53.4110, -1.5000, 0.9),
               ("Broomhill", 53.3790, -1.4960, 0.8), ("Attercliffe", 53.3910, -1.4260, 0.9),
               ("Heeley", 53.3490, -1.4600, 0.8)],
     "motorways": ["M1", "M18"],
     "a_roads": ["A57", "A61", "A630", "A6102"],
     "hotspots": [
         {"area": "Attercliffe", "dlat": 0.003, "dlon": 0.003, "road_name": "A6178 Attercliffe Road",
          "road_type": "A Road", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "City Centre", "dlat": -0.003, "dlon": -0.003, "road_name": "A61 Inner Ring Road",
          "road_type": "Dual Carriageway", "junction_type": "Roundabout", "weight": 1.0}]},

    {"name": "Edinburgh", "weight": 7, "center": (55.9533, -3.1883),
     "climate": (1.0, 1.3, -1.0), "rural_bearing": (80, 280),
     "areas": [("Old Town", 55.9496, -3.1914, 1.4), ("Leith", 55.9760, -3.1700, 1.0),
               ("Morningside", 55.9260, -3.2080, 0.8), ("Gorgie", 55.9410, -3.2400, 0.9),
               ("Portobello", 55.9570, -3.1150, 0.8)],
     "motorways": ["M8", "M9", "M90"],
     "a_roads": ["A90", "A8", "A1", "A702", "A71"],
     "hotspots": [
         {"area": "Gorgie", "dlat": 0.003, "dlon": 0.003, "road_name": "A71 Slateford Road",
          "road_type": "A Road", "junction_type": "Traffic Signal Junction", "weight": 1.0},
         {"area": "Old Town", "dlat": -0.002, "dlon": -0.004, "road_name": "A700 Lothian Road",
          "road_type": "A Road", "junction_type": "Crossroads", "weight": 1.0}]},
]

# Helper lookups so the generator does not have to search lists (built once at import time).
for _city in CITIES:
    _city["area_by_name"] = {a[0]: a for a in _city["areas"]}
    _city["area_weights"] = [a[3] for a in _city["areas"]]

STREET_NAMES = ["High Street", "Station Road", "Church Street", "Victoria Road", "London Road",
                "Queens Road", "Park Lane", "Mill Lane", "New Road", "King Street",
                "Bridge Street", "Albert Road", "Manor Road", "Kings Road", "Oxford Road",
                "Market Street", "Chapel Lane", "Green Lane", "Priory Road", "Grange Road"]
LANE_NAMES = ["Mill Lane", "Green Lane", "Chapel Lane", "Manor Lane", "Back Lane",
              "Hill Road", "Common Lane", "Cross Lane", "Church Lane", "Farm Road"]

# =====================================================================
# 4. WEATHER
# =====================================================================
WEATHER_TYPES = ["Clear", "Cloudy", "Rain", "Heavy Rain", "Fog", "Snow", "Windy"]

# Relative chance of each weather type per month (same order as WEATHER_TYPES).
# Snow is only possible from November to April, fog peaks in autumn/winter.
WEATHER_BY_MONTH = {
    1: [20, 33, 20, 4, 7, 6, 10],     2: [22, 31, 19, 4, 5, 6, 13],
    3: [28, 30, 18, 3, 3, 2, 16],     4: [36, 29, 17, 3, 2, 0.3, 12.7],
    5: [44, 27, 15, 3, 1, 0, 10],     6: [48, 25, 14, 3, 1, 0, 9],
    7: [50, 24, 14, 3, 1, 0, 8],      8: [48, 25, 15, 3, 1, 0, 8],
    9: [40, 27, 16, 3, 3, 0, 11],     10: [30, 29, 19, 4, 7, 0, 11],
    11: [24, 31, 20, 4, 9, 2, 10],    12: [20, 33, 20, 4, 8, 5, 10],
}

# Bad weather days produce MORE accidents in that city on that day.
WEATHER_ACCIDENT_MULT = {"Clear": 1.0, "Cloudy": 1.0, "Rain": 1.25, "Heavy Rain": 1.45,
                         "Fog": 1.20, "Snow": 1.35, "Windy": 1.05}

MONTH_MEAN_TEMP = {1: 4.5, 2: 5.0, 3: 7.0, 4: 9.5, 5: 12.5, 6: 15.5,
                   7: 17.5, 8: 17.0, 9: 14.5, 10: 11.0, 11: 7.5, 12: 5.0}
TEMP_WEATHER_ADJUST = {"Clear": 1.0, "Cloudy": 0.0, "Rain": -0.3, "Heavy Rain": -0.3,
                       "Fog": -1.0, "Snow": 0.0, "Windy": -0.5}

# Typical measurements for each weather type.
#   humidity       = (mean %, standard deviation)   humidity_floor = lowest allowed value
#   visibility     = (min km, max km, most likely km)
#   wind           = (mean km/h, standard deviation) wind_min = lowest allowed value
WEATHER_PROFILES = {
    "Clear":      {"humidity": (58, 12), "humidity_floor": 25, "visibility": (10, 35, 22), "wind": (10, 5), "wind_min": 0},
    "Cloudy":     {"humidity": (72, 9),  "humidity_floor": 30, "visibility": (6, 25, 14),  "wind": (14, 6), "wind_min": 0},
    "Rain":       {"humidity": (84, 6),  "humidity_floor": 68, "visibility": (3, 10, 6),   "wind": (20, 8), "wind_min": 0},
    "Heavy Rain": {"humidity": (91, 4),  "humidity_floor": 80, "visibility": (0.8, 4, 2),  "wind": (30, 10), "wind_min": 5},
    "Fog":        {"humidity": (95, 3),  "humidity_floor": 88, "visibility": (0.03, 0.95, 0.30), "wind": (5, 3), "wind_min": 0},
    "Snow":       {"humidity": (87, 5),  "humidity_floor": 70, "visibility": (0.4, 5, 1.8), "wind": (20, 9), "wind_min": 0},
    "Windy":      {"humidity": (68, 10), "humidity_floor": 30, "visibility": (7, 25, 15),  "wind": (48, 10), "wind_min": 35},
}
ADVERSE_WEATHER = ("Rain", "Heavy Rain", "Fog", "Snow", "Windy")

# =====================================================================
# 5. ROADS
# =====================================================================
ROAD_TYPE_WEIGHTS = {
    "Urban": {"Local Street": 36, "A Road": 24, "Dual Carriageway": 16, "B Road": 10, "Motorway": 14},
    "Rural": {"A Road": 30, "B Road": 24, "Rural Lane": 22, "Motorway": 14, "Dual Carriageway": 10},
}

# Speed limits are stored in km/h (rounded UK equivalents: 32=20mph, 48=30mph, 64=40mph,
# 80=50mph, 96=60mph, 112=70mph) so they compare directly with vehicle_speed_kmh.
# Format: (road type, urban/rural) -> ([limits], [weights])
SPEED_LIMITS = {
    ("Motorway", "Urban"): ([112, 96, 80], [75, 15, 10]),
    ("Motorway", "Rural"): ([112, 96, 80], [85, 10, 5]),
    ("Dual Carriageway", "Urban"): ([64, 80, 96], [45, 35, 20]),
    ("Dual Carriageway", "Rural"): ([96, 112], [40, 60]),
    ("A Road", "Urban"): ([48, 64, 80], [55, 30, 15]),
    ("A Road", "Rural"): ([80, 96], [30, 70]),
    ("B Road", "Urban"): ([48, 64], [70, 30]),
    ("B Road", "Rural"): ([64, 80, 96], [20, 30, 50]),
    ("Local Street", "Urban"): ([32, 48], [40, 60]),
    ("Local Street", "Rural"): ([48, 64], [60, 40]),
    ("Rural Lane", "Urban"): ([48, 64], [60, 40]),
    ("Rural Lane", "Rural"): ([48, 64, 96], [20, 30, 50]),
}

# Base junction weights; the generator adjusts them for road type (see generators.py).
JUNCTION_WEIGHTS = {
    "Urban": {"No Junction": 34, "T-Junction": 22, "Crossroads": 13, "Roundabout": 12,
              "Traffic Signal Junction": 13, "Private Drive Entrance": 6},
    "Rural": {"No Junction": 55, "T-Junction": 24, "Crossroads": 8, "Roundabout": 6,
              "Private Drive Entrance": 7},
}
MOTORWAY_JUNCTION_WEIGHTS = {"No Junction": 80, "Slip Road": 20}

ROAD_LIGHTING_WEIGHTS = {
    "Urban": {"Present - Working": 88, "Present - Not Working": 4, "No Street Lighting": 8},
    "Rural": {"Present - Working": 12, "Present - Not Working": 2, "No Street Lighting": 86},
    "RuralMotorway": {"Present - Working": 25, "Present - Not Working": 2, "No Street Lighting": 73},
}
ROAD_WORKS_PROBABILITY = {"Urban": 0.07, "Rural": 0.04, "Motorway": 0.12}

# Traffic density depends on the period of the day and on urban/rural.
# Order of every list: Free Flow, Moderate, Heavy, Congested.
TRAFFIC_CONDITIONS = ["Free Flow", "Moderate", "Heavy", "Congested"]
TRAFFIC_WEIGHTS = {
    ("rush", "Urban"): [5, 20, 45, 30],   ("rush", "Rural"): [35, 40, 20, 5],
    ("day", "Urban"): [20, 50, 25, 5],    ("day", "Rural"): [55, 35, 8, 2],
    ("night", "Urban"): [82, 15, 3, 0],   ("night", "Rural"): [93, 6, 1, 0],
}
# How fast vehicles typically travel compared with the speed limit in each traffic state.
TRAFFIC_SPEED_FACTOR = {"Free Flow": 1.0, "Moderate": 0.85, "Heavy": 0.6, "Congested": 0.35}

# =====================================================================
# 6. VEHICLES
# =====================================================================
VEHICLE_TYPES = ["Car", "Motorcycle", "Truck", "Bus", "Bicycle", "Van"]
VEHICLE_TYPE_BASE = {"Car": 62, "Van": 9, "Motorcycle": 11, "Truck": 7, "Bus": 3, "Bicycle": 8}
SEATBELT_VEHICLES = ("Car", "Van", "Truck")      # vehicles that have seatbelts in this generator
HELMET_VEHICLES = ("Motorcycle", "Bicycle")      # vehicles where helmet use is relevant

# How many vehicles are involved (before adjustments for motorways, fog, pedestrians...).
VEHICLE_COUNT_WEIGHTS = {1: 38, 2: 48, 3: 9.5, 4: 3.0, 5: 1.0, 6: 0.5}

# (make, model, engine capacity cc). Bicycles have no engine (cc = None).
VEHICLE_CATALOG = {
    "Car": [("Ford", "Fiesta", 998), ("Ford", "Focus", 1498), ("Ford", "Kuga", 1995),
            ("Vauxhall", "Corsa", 1199), ("Vauxhall", "Astra", 1399), ("Vauxhall", "Mokka", 1199),
            ("Volkswagen", "Polo", 999), ("Volkswagen", "Golf", 1498), ("Volkswagen", "Passat", 1968),
            ("Volkswagen", "Tiguan", 1984), ("BMW", "1 Series", 1499), ("BMW", "3 Series", 1998),
            ("BMW", "X3", 1998), ("Audi", "A3", 1498), ("Audi", "A4", 1984), ("Audi", "Q5", 1984),
            ("Toyota", "Yaris", 1490), ("Toyota", "Corolla", 1798), ("Toyota", "RAV4", 2487),
            ("Nissan", "Micra", 999), ("Nissan", "Juke", 999), ("Nissan", "Qashqai", 1332),
            ("Honda", "Jazz", 1498), ("Honda", "Civic", 1498), ("Honda", "CR-V", 1993),
            ("Hyundai", "i10", 998), ("Hyundai", "i20", 998), ("Hyundai", "Tucson", 1598),
            ("Kia", "Picanto", 998), ("Kia", "Ceed", 1353), ("Kia", "Sportage", 1598),
            ("Mercedes-Benz", "A-Class", 1332), ("Mercedes-Benz", "C-Class", 1993),
            ("Peugeot", "208", 1199), ("Peugeot", "308", 1199), ("Renault", "Clio", 999),
            ("Mini", "Cooper", 1499)],
    "Van": [("Ford", "Transit", 1996), ("Ford", "Transit Custom", 1996),
            ("Mercedes-Benz", "Sprinter", 2143), ("Vauxhall", "Vivaro", 1499),
            ("Volkswagen", "Transporter", 1968), ("Renault", "Trafic", 1997),
            ("Citroen", "Berlingo", 1499), ("Peugeot", "Partner", 1499)],
    "Motorcycle": [("Honda", "PCX125", 125), ("Honda", "CB500F", 471), ("Yamaha", "MT-07", 689),
                   ("Yamaha", "YZF-R125", 125), ("Kawasaki", "Ninja 650", 649),
                   ("Kawasaki", "Z900", 948), ("Suzuki", "GSX-S750", 749), ("BMW", "R1250GS", 1254),
                   ("Triumph", "Street Triple", 765), ("Ducati", "Monster", 937),
                   ("Harley-Davidson", "Sportster S", 1252), ("Vespa", "Primavera 125", 125)],
    "Truck": [("Volvo", "FH", 12777), ("Scania", "R450", 12742), ("DAF", "XF", 12902),
              ("MAN", "TGX", 12419), ("Mercedes-Benz", "Actros", 12809), ("Iveco", "S-Way", 12882)],
    "Bus": [("Alexander Dennis", "Enviro400", 6690), ("Volvo", "B5LH", 5100),
            ("Wrightbus", "StreetDeck", 4500), ("Mercedes-Benz", "Citaro", 7700),
            ("MAN", "Lion's City", 10518)],
    "Bicycle": [("Trek", "FX 2", None), ("Specialized", "Sirrus", None), ("Giant", "Escape 3", None),
                ("Brompton", "M6L", None), ("Raleigh", "Pioneer", None),
                ("Cannondale", "Quick 4", None), ("Boardman", "MTX 8.6", None)],
}
VEHICLE_MEAN_AGE = {"Car": 8.5, "Van": 7.5, "Motorcycle": 9.0, "Truck": 6.5, "Bus": 8.0, "Bicycle": 5.0}
VEHICLE_MAX_AGE = {"Car": 25, "Van": 22, "Motorcycle": 22, "Truck": 20, "Bus": 20, "Bicycle": 15}
VEHICLE_COLORS = {"Black": 20, "White": 19, "Silver": 16, "Grey": 17, "Blue": 11,
                  "Red": 9, "Green": 3, "Yellow": 1, "Orange": 1, "Brown": 3}
VEHICLE_DIRECTIONS = ["North", "South", "East", "West", "North-East", "North-West",
                      "South-East", "South-West"]
VEHICLE_DIRECTION_WEIGHTS = [20, 20, 17, 17, 6, 6, 7, 7]
VEHICLE_MANEUVERS = ["Going Ahead", "Turning Left", "Turning Right", "Overtaking", "Changing Lane",
                     "Slowing/Stopping", "Waiting to Turn", "Reversing", "U-Turn", "Parked"]
BASE_MANEUVER_WEIGHTS = {"Going Ahead": 52, "Turning Right": 8, "Turning Left": 6, "Overtaking": 5,
                         "Changing Lane": 6, "Slowing/Stopping": 8, "Waiting to Turn": 5,
                         "Reversing": 2, "U-Turn": 1, "Parked": 3}
DAMAGE_LEVELS = ["No Damage", "Minor", "Moderate", "Severe", "Total Loss"]
DAMAGE_TYPE_ADJUST = {"Car": 0.0, "Van": 0.0, "Motorcycle": 0.5, "Truck": -0.6, "Bus": -0.6, "Bicycle": 0.2}
DRIVER_CONDITIONS = ["Normal", "Alcohol Impaired", "Drug Impaired", "Fatigued", "Distracted"]

# Share of male drivers per vehicle type.
MALE_SHARE = {"Car": 0.58, "Van": 0.88, "Motorcycle": 0.90, "Truck": 0.96, "Bus": 0.82, "Bicycle": 0.74}
# Youngest realistic driver per vehicle type (bicycle = age at which people start cycling).
MIN_LICENCE_AGE = {"Car": 17, "Van": 18, "Motorcycle": 17, "Truck": 21, "Bus": 24, "Bicycle": 6}

# Age bands (low, high, weight) for the driver / rider of each vehicle type.
_STANDARD_BANDS = [(17, 24, 22), (25, 34, 22), (35, 44, 17), (45, 54, 14), (55, 64, 11), (65, 74, 8), (75, 85, 6)]
DRIVER_AGE_BANDS = {
    "Car": _STANDARD_BANDS,
    "Van": [(18, 24, 14), (25, 34, 26), (35, 44, 24), (45, 54, 20), (55, 64, 13), (65, 70, 3)],
    "Motorcycle": [(17, 24, 24), (25, 34, 26), (35, 44, 22), (45, 54, 16), (55, 64, 10), (65, 72, 2)],
    "Truck": [(21, 29, 12), (30, 39, 24), (40, 49, 28), (50, 59, 26), (60, 64, 10)],
    "Bus": [(24, 34, 12), (35, 44, 26), (45, 54, 32), (55, 66, 30)],
    "Bicycle": [(8, 16, 16), (17, 24, 14), (25, 34, 22), (35, 44, 18), (45, 54, 15), (55, 64, 9), (65, 80, 6)],
}
PASSENGER_AGE_BANDS = [(0, 12, 16), (13, 17, 8), (18, 24, 14), (25, 44, 26), (45, 64, 22), (65, 90, 14)]
PEDESTRIAN_AGE_BANDS = [(5, 15, 22), (16, 24, 14), (25, 44, 22), (45, 64, 20), (65, 90, 22)]

# Number of passengers travelling with the driver (Bus is handled separately in code).
PASSENGER_COUNT_WEIGHTS = {
    "Car": {0: 58, 1: 26, 2: 9, 3: 5, 4: 2},
    "Van": {0: 72, 1: 22, 2: 6},
    "Truck": {0: 90, 1: 10},
    "Motorcycle": {0: 88, 1: 12},
    "Bicycle": {0: 100},
}

# =====================================================================
# 7. SEVERITY, CASUALTIES AND CAUSES
# =====================================================================
SEVERITIES = ["Fatal", "Serious", "Slight"]
# Intercepts of the two logistic models that turn a "risk score" into a severity.
# Tuned so that, overall, roughly 80% Slight / 17-18% Serious / 2-3% Fatal.
SEVERITY_INTERCEPT_SERIOUS = -2.45
SEVERITY_INTERCEPT_FATAL = -3.70

# How many people are injured, depending on the accident severity.
CASUALTY_COUNT_WEIGHTS = {
    "Slight": {1: 62, 2: 24, 3: 9, 4: 3, 5: 2},
    "Serious": {1: 50, 2: 28, 3: 12, 4: 6, 5: 3, 6: 1},
    "Fatal": {1: 48, 2: 28, 3: 13, 4: 6, 5: 3, 6: 2},
}
CASUALTY_TYPES = ["Driver", "Passenger", "Pedestrian", "Cyclist", "Motorcyclist"]
# How likely each person is to be among the injured (riders are almost always hurt).
ROLE_PICK_WEIGHT = {"Driver": 1.5, "Motorcyclist": 4.0, "Cyclist": 4.0, "Passenger": 0.8}
GENDERS = ["Male", "Female"]

PRIMARY_CAUSES = ["Drunk Driving", "Speeding", "Reckless Driving", "Distracted Driving",
                  "Driver Fatigue", "Adverse Weather", "Poor Road Condition", "Traffic Violation",
                  "Failed to Look Properly", "Misjudged Distance or Speed", "Mechanical Failure",
                  "Pedestrian/Cyclist Error", "Animal on Road", "Undetermined"]
# Boolean factor -> (primary cause label, weight when several factors are present)
FLAG_TO_CAUSE = {
    "drunk_driving": ("Drunk Driving", 5.0),
    "speeding_involved": ("Speeding", 4.0),
    "reckless_driving": ("Reckless Driving", 3.5),
    "distracted_driving": ("Distracted Driving", 3.0),
    "driver_fatigue": ("Driver Fatigue", 2.5),
    "weather_related": ("Adverse Weather", 2.0),
    "poor_road_condition": ("Poor Road Condition", 2.0),
    "traffic_violation": ("Traffic Violation", 2.0),
}

LIGHT_CONDITIONS = ["Daylight", "Dawn/Dusk", "Darkness - Lights Lit", "Darkness - No Lighting"]
ROAD_TYPES = ["Motorway", "Dual Carriageway", "A Road", "B Road", "Local Street", "Rural Lane"]
JUNCTION_TYPES = ["No Junction", "T-Junction", "Crossroads", "Roundabout", "Traffic Signal Junction",
                  "Slip Road", "Private Drive Entrance"]
ROAD_LIGHTING = ["Present - Working", "Present - Not Working", "No Street Lighting"]
ROAD_SURFACES = ["Dry", "Wet", "Icy", "Snowy", "Muddy"]
ROAD_HAZARDS = ["No Hazard", "Pothole", "Debris on Road", "Oil/Diesel Spill", "Stationary Vehicle",
                "Animal on Road", "Flooding", "Fallen Tree", "Ice Patch"]

# Probability of a sensor / reporting gap (kept small: no excessive missing data).
MISSING_WEATHER_RATE = 0.008      # humidity, visibility, wind speed
MISSING_TEMPERATURE_RATE = 0.005
UNKNOWN_DRIVER_RATE = 0.015       # e.g. hit-and-run: driver age/gender/experience unknown
MISSING_COLOR_RATE = 0.02
MISSING_CASUALTY_AGE_RATE = 0.01

# =====================================================================
# 8. TABLE SCHEMAS  (column order = CSV column order = data dictionary)
#    (column name, data type, description)
# =====================================================================
ACCIDENT_SCHEMA = [
    ("accident_id", "string", "Primary key. Synthetic accident identifier such as ACC0000001; IDs grow in chronological order."),
    ("accident_date", "date (YYYY-MM-DD)", "Date of the synthetic accident."),
    ("accident_time", "time (HH:MM:SS)", "Local time of day of the accident."),
    ("day_of_week", "string", "Day name, Monday to Sunday."),
    ("month", "integer", "Month number, 1 (January) to 12 (December)."),
    ("year", "integer", "Calendar year of the accident."),
    ("country", "string", "Country of the accident (always United Kingdom in this generator)."),
    ("city", "string", "City the accident is attributed to."),
    ("area", "string", "District inside the city for urban accidents, or 'Outskirts <North/East/South/West>' for rural ones."),
    ("latitude", "float", "Synthetic WGS84 latitude in decimal degrees, inside the city's bounding box."),
    ("longitude", "float", "Synthetic WGS84 longitude in decimal degrees, inside the city's bounding box."),
    ("road_name", "string", "Road on which the accident occurred (motorway/A-road number or street name)."),
    ("road_type", "string", "Motorway, Dual Carriageway, A Road, B Road, Local Street or Rural Lane."),
    ("urban_or_rural", "string", "Urban (built-up city area) or Rural (outskirts)."),
    ("junction_type", "string", "Junction at the accident site, or 'No Junction'."),
    ("speed_limit", "integer", "Posted speed limit in km/h (rounded UK limits: 32, 48, 64, 80, 96, 112)."),
    ("accident_severity", "string", "Fatal, Serious or Slight. Equals the worst injury among the casualties."),
    ("number_of_vehicles", "integer", "Number of vehicle records for this accident in Vehicles.csv."),
    ("number_of_casualties", "integer", "Number of casualty records for this accident in Casualties.csv."),
    ("accident_duration_minutes", "integer", "Minutes until the scene was cleared and traffic flowed again."),
    ("traffic_conditions", "string", "Traffic density at the time: Free Flow, Moderate, Heavy or Congested."),
    ("light_conditions", "string", "Daylight, Dawn/Dusk, Darkness - Lights Lit or Darkness - No Lighting (derived from sun position)."),
    ("weather_condition", "string", "Clear, Cloudy, Rain, Heavy Rain, Fog, Snow or Windy."),
    ("temperature_celsius", "float", "Air temperature in degrees Celsius. Missing for a small share of rows (sensor gap)."),
    ("humidity_percentage", "float", "Relative humidity, 0-100. Missing for a small share of rows (sensor gap)."),
    ("visibility_km", "float", "Visibility distance in kilometres. Missing for a small share of rows (sensor gap)."),
    ("wind_speed_kmh", "float", "Wind speed in km/h. Missing for a small share of rows (sensor gap)."),
    ("precipitation_mm", "float", "Precipitation intensity in mm per hour (0 when dry)."),
    ("road_surface_condition", "string", "Dry, Wet, Icy, Snowy or Muddy."),
    ("road_lighting", "string", "Street lighting at the site: Present - Working, Present - Not Working or No Street Lighting."),
    ("road_hazards", "string", "Hazard on the road (No Hazard, Pothole, Debris on Road, Oil/Diesel Spill, Stationary Vehicle, Animal on Road, Flooding, Fallen Tree, Ice Patch)."),
    ("road_works", "boolean", "True if road works were present at the site."),
    ("primary_cause", "string", "Single main cause chosen from the contributing factors (or a non-flag cause when no factor applies)."),
    ("speeding_involved", "boolean", "True if at least one motor vehicle was travelling above the speed limit."),
    ("distracted_driving", "boolean", "True if a driver was distracted (phone, passengers, etc.)."),
    ("drunk_driving", "boolean", "True if a driver was impaired by alcohol or drugs."),
    ("reckless_driving", "boolean", "True if reckless or aggressive driving was involved."),
    ("poor_road_condition", "boolean", "True if road surface or hazards contributed to the accident."),
    ("weather_related", "boolean", "True if adverse weather (rain, fog, snow, wind) contributed."),
    ("traffic_violation", "boolean", "True if a traffic rule was broken (e.g. red light, illegal turn, speeding)."),
    ("driver_fatigue", "boolean", "True if a driver was fatigued."),
]

VEHICLE_SCHEMA = [
    ("vehicle_id", "string", "Primary key. Synthetic vehicle identifier such as VEH00000001."),
    ("accident_id", "string", "Foreign key to Accidents.accident_id."),
    ("vehicle_reference", "integer", "Position of the vehicle inside its accident (1, 2, 3, ...). Vehicle 1 is the main vehicle."),
    ("vehicle_type", "string", "Car, Motorcycle, Truck, Bus, Bicycle or Van."),
    ("vehicle_make", "string", "Manufacturer (synthetic catalogue)."),
    ("vehicle_model", "string", "Model name that belongs to the make and vehicle type."),
    ("vehicle_year", "integer", "Year of manufacture (never later than the accident year)."),
    ("vehicle_age", "integer", "Accident year minus vehicle_year, in years."),
    ("vehicle_speed_kmh", "integer", "Estimated speed just before the collision in km/h (0 for parked or waiting vehicles)."),
    ("vehicle_direction", "string", "Compass direction of travel."),
    ("vehicle_maneuver", "string", "What the vehicle was doing (Going Ahead, Turning Left, Overtaking, Parked, ...)."),
    ("vehicle_damage_severity", "string", "No Damage, Minor, Moderate, Severe or Total Loss."),
    ("driver_age", "integer", "Age of the driver/rider. Empty when the driver is unknown (e.g. hit-and-run)."),
    ("driver_gender", "string", "Male or Female. Empty when the driver is unknown."),
    ("driver_experience_years", "integer", "Years since the driver started driving/riding (licence age). Empty when the driver is unknown."),
    ("seatbelt_used", "boolean", "Only for vehicles with seatbelts (Car, Van, Truck). Empty = not applicable."),
    ("helmet_used", "boolean", "Only for Motorcycle and Bicycle. Empty = not applicable."),
    ("driver_condition", "string", "Normal, Alcohol Impaired, Drug Impaired, Fatigued or Distracted."),
    ("vehicle_color", "string", "Vehicle colour. Empty for a small share of rows."),
    ("engine_capacity_cc", "integer", "Engine capacity in cubic centimetres. Empty for bicycles (no engine)."),
]

CASUALTY_SCHEMA = [
    ("casualty_id", "string", "Primary key. Synthetic casualty identifier such as CAS00000001."),
    ("accident_id", "string", "Foreign key to Accidents.accident_id."),
    ("vehicle_id", "string", "Foreign key to Vehicles.vehicle_id. Empty (NULL) for pedestrians."),
    ("casualty_type", "string", "Driver, Passenger, Pedestrian, Cyclist or Motorcyclist."),
    ("age", "integer", "Age of the casualty. Drivers/riders share the age of the vehicle's driver. Small share missing."),
    ("gender", "string", "Male or Female. Empty when unknown."),
    ("injury_severity", "string", "Fatal, Serious or Slight."),
    ("hospitalized", "boolean", "True if the casualty was taken to hospital."),
    ("emergency_response_required", "boolean", "True if an ambulance / emergency service response was required."),
    ("seatbelt_used", "boolean", "Only for occupants of cars, vans and trucks. Empty = not applicable."),
    ("helmet_used", "boolean", "Only for motorcyclists, cyclists and motorcycle passengers. Empty = not applicable."),
]

ACCIDENT_COLUMNS = [c[0] for c in ACCIDENT_SCHEMA]
VEHICLE_COLUMNS = [c[0] for c in VEHICLE_SCHEMA]
CASUALTY_COLUMNS = [c[0] for c in CASUALTY_SCHEMA]

# Allowed values per categorical column (used by the validation step).
ACCIDENT_CATEGORIES = {
    "accident_severity": SEVERITIES, "weather_condition": WEATHER_TYPES,
    "road_surface_condition": ROAD_SURFACES, "road_type": ROAD_TYPES,
    "urban_or_rural": ["Urban", "Rural"], "junction_type": JUNCTION_TYPES,
    "traffic_conditions": TRAFFIC_CONDITIONS, "light_conditions": LIGHT_CONDITIONS,
    "road_lighting": ROAD_LIGHTING, "road_hazards": ROAD_HAZARDS,
    "primary_cause": PRIMARY_CAUSES, "city": [c["name"] for c in CITIES],
    "day_of_week": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
}
VEHICLE_CATEGORIES = {
    "vehicle_type": VEHICLE_TYPES, "vehicle_direction": VEHICLE_DIRECTIONS,
    "vehicle_maneuver": VEHICLE_MANEUVERS, "vehicle_damage_severity": DAMAGE_LEVELS,
    "driver_gender": GENDERS, "driver_condition": DRIVER_CONDITIONS,
}
CASUALTY_CATEGORIES = {
    "casualty_type": CASUALTY_TYPES, "gender": GENDERS, "injury_severity": SEVERITIES,
}
