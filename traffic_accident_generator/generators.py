"""
generators.py
=============
The data generation logic for the SYNTHETIC Traffic Accident Dataset.

All output is SYNTHETIC: it is produced by probability rules, not collected from
real accidents.

How one accident is built (read build_one_accident() first - it is the "recipe"):

    1. WHEN / WHERE   date, hour, city        (chosen in bulk by build_timeline)
    2. LOCATION       area, coordinates, hotspot
    3. ROAD           road type/name, junction, speed limit, lighting, road works
    4. WEATHER        temperature, humidity, visibility, wind, rain, road surface
    5. CONTEXT        light conditions, traffic density, hazards
    6. PEOPLE         pedestrian involved? how many vehicles? which vehicle types?
    7. FACTORS        speeding / drunk / distracted / ... (depend on steps 1-6)
    8. VEHICLES       speed, driver age, seatbelt, helmet ...
    9. SEVERITY       risk score -> Slight / Serious / Fatal
   10. CASUALTIES     who was hurt and how badly (consistent with the severity)

Each step only uses information from the earlier steps, so the final record is
internally consistent.
"""
import itertools
import math
import random

import numpy as np
import pandas as pd

import config as cfg


# =====================================================================
# 0. Small helper functions
# =====================================================================
def pick(rng, options):
    """Pick one key from a {value: weight} dictionary."""
    return rng.choices(list(options.keys()), weights=list(options.values()), k=1)[0]


def chance(rng, probability):
    """Return True with the given probability (0.0 - 1.0)."""
    return rng.random() < probability


def clamp(value, low, high):
    """Keep a number between low and high."""
    return max(low, min(high, value))


def sigmoid(x):
    """Turn any number into a probability between 0 and 1."""
    return 1.0 / (1.0 + math.exp(-x))


def compass_name(bearing):
    """Convert a compass bearing in degrees to North / East / South / West."""
    bearing = bearing % 360
    if bearing >= 315 or bearing < 45:
        return "North"
    if bearing < 135:
        return "East"
    if bearing < 225:
        return "South"
    return "West"


# =====================================================================
# 1. TIMELINE: when does each accident happen, and in which city?
# =====================================================================
def build_timeline(num_accidents, rng, np_rng):
    """
    Choose the date, time and city of every accident in bulk (fast, numpy based).

    * First a weather type and a base temperature are drawn for every (city, day).
    * Every (city, day) gets an "accident weight": busy months, Fridays, big cities and
      bad-weather days get more accidents.
    * Accidents are then sampled according to those weights, and the hour of the day is
      sampled from the weekday or weekend profile (rush hours on weekdays).
    * Finally the accidents are sorted by time and receive sequential IDs, so replaying
      them in ID order replays them in chronological order (handy for Kafka).
    """
    dates = pd.date_range(cfg.START_DATE, cfg.END_DATE, freq="D")
    n_days, n_cities = len(dates), len(cfg.CITIES)

    # Weight of each day that does not depend on the city.
    day_weight = np.array([
        cfg.MONTH_WEIGHTS[d.month - 1]
        * cfg.DAY_OF_WEEK_WEIGHTS[d.dayofweek]
        * (1 + cfg.YEARLY_TREND * (d.year - cfg.START_DATE.year))
        for d in dates
    ])

    weather = np.empty((n_cities, n_days), dtype=object)
    day_temp = np.zeros((n_cities, n_days))
    weight = np.zeros((n_cities, n_days))

    for ci, city in enumerate(cfg.CITIES):
        rain_mult, snow_mult, temp_adj = city["climate"]
        for di, d in enumerate(dates):
            probs = dict(zip(cfg.WEATHER_TYPES, cfg.WEATHER_BY_MONTH[d.month]))
            probs["Rain"] *= rain_mult
            probs["Heavy Rain"] *= rain_mult
            probs["Snow"] *= snow_mult
            kind = pick(rng, probs)

            temp = (cfg.MONTH_MEAN_TEMP[d.month] + temp_adj
                    + cfg.TEMP_WEATHER_ADJUST[kind] + rng.gauss(0, 3.0))
            if kind == "Snow":                      # snow needs freezing-ish temperatures
                temp = min(temp, rng.uniform(-3.5, 1.5))

            weather[ci, di] = kind
            day_temp[ci, di] = temp
            weight[ci, di] = city["weight"] * day_weight[di] * cfg.WEATHER_ACCIDENT_MULT[kind]

    # Sample (city, day) pairs according to their weights.
    probabilities = weight.ravel() / weight.sum()
    chosen = np_rng.choice(probabilities.size, size=num_accidents, p=probabilities)
    city_idx, day_idx = np.divmod(chosen, n_days)

    # Sample the hour: weekdays and weekends have different profiles.
    is_weekend = np.array([d.dayofweek >= 5 for d in dates])[day_idx]
    p_weekday = np.array(cfg.HOUR_WEIGHTS_WEEKDAY, dtype=float)
    p_weekend = np.array(cfg.HOUR_WEIGHTS_WEEKEND, dtype=float)
    hours = np.where(is_weekend,
                     np_rng.choice(24, num_accidents, p=p_weekend / p_weekend.sum()),
                     np_rng.choice(24, num_accidents, p=p_weekday / p_weekday.sum()))

    timeline = pd.DataFrame({
        "city_idx": city_idx,
        "date": dates[day_idx],
        "hour": hours,
        "minute": np_rng.integers(0, 60, num_accidents),
        "second": np_rng.integers(0, 60, num_accidents),
        "weather": weather[city_idx, day_idx],
        "day_temp": day_temp[city_idx, day_idx],
    })
    timeline = (timeline.sort_values(["date", "hour", "minute", "second", "city_idx"], kind="stable")
                .reset_index(drop=True))
    timeline["accident_id"] = [f"ACC{i:07d}" for i in range(1, num_accidents + 1)]
    return timeline


# =====================================================================
# 2. LOCATION
# =====================================================================
def sample_location(city, urban, rng):
    """
    Return (area, latitude, longitude, hotspot) for one accident.

    * Urban accidents cluster around the city's areas (busy areas get more accidents).
      A share of them falls on one of the city's two hotspot junctions, where accidents
      are packed very tightly - these are the "high-risk locations" analytics should find.
    * Rural accidents are placed 10-24 km from the city centre in a random direction.
    """
    center_lat, center_lon = city["center"]
    hotspot = None

    if urban:
        if chance(rng, cfg.HOTSPOT_SHARE):
            hotspot = rng.choices(city["hotspots"],
                                  weights=[h["weight"] for h in city["hotspots"]], k=1)[0]
            _, area_lat, area_lon, _ = city["area_by_name"][hotspot["area"]]
            lat = area_lat + hotspot["dlat"] + rng.gauss(0, cfg.HOTSPOT_SPREAD_DEG)
            lon = area_lon + hotspot["dlon"] + rng.gauss(0, cfg.HOTSPOT_SPREAD_DEG)
            area = hotspot["area"]
        else:
            name, area_lat, area_lon, _ = rng.choices(city["areas"],
                                                      weights=city["area_weights"], k=1)[0]
            lat = rng.gauss(area_lat, cfg.AREA_SPREAD_LAT)
            lon = rng.gauss(area_lon, cfg.AREA_SPREAD_LON)
            area = name
    else:
        low, high = city.get("rural_bearing", (0, 360))
        bearing = rng.uniform(low, high)
        distance_km = rng.uniform(cfg.RURAL_MIN_KM, cfg.RURAL_MAX_KM)
        lat = center_lat + distance_km * math.cos(math.radians(bearing)) / 111.0
        lon = center_lon + (distance_km * math.sin(math.radians(bearing))
                            / (111.0 * math.cos(math.radians(center_lat))))
        area = "Outskirts " + compass_name(bearing)

    # Safety net: never leave the city's bounding box.
    lat = clamp(lat, center_lat - cfg.BBOX_HALF_LAT, center_lat + cfg.BBOX_HALF_LAT)
    lon = clamp(lon, center_lon - cfg.BBOX_HALF_LON, center_lon + cfg.BBOX_HALF_LON)
    return area, round(lat, 6), round(lon, 6), hotspot


# =====================================================================
# 3. ROAD
# =====================================================================
def make_road_name(city, road_type, rng):
    """Create a plausible road name for the road type."""
    if road_type == "Motorway":
        return rng.choice(city["motorways"])
    if road_type in ("A Road", "Dual Carriageway"):
        return rng.choice(city["a_roads"])
    if road_type == "B Road":
        return f"B{rng.randint(3000, 4999)}"
    if road_type == "Rural Lane":
        return rng.choice(cfg.LANE_NAMES)
    return rng.choice(cfg.STREET_NAMES)            # Local Street


def sample_junction(road_type, urban, rng):
    """Junction type depends on the road type and on urban/rural."""
    if road_type == "Motorway":
        return pick(rng, cfg.MOTORWAY_JUNCTION_WEIGHTS)
    weights = dict(cfg.JUNCTION_WEIGHTS["Urban" if urban else "Rural"])
    if road_type == "Dual Carriageway":            # more roundabouts, fewer side junctions
        weights["Roundabout"] = weights.get("Roundabout", 0) * 1.5
        weights["T-Junction"] *= 0.5
    elif road_type == "Local Street" and urban:    # many side streets
        weights["T-Junction"] *= 1.3
        weights["Traffic Signal Junction"] *= 0.4
    elif road_type == "Rural Lane":
        weights["Roundabout"] = 0
    return pick(rng, weights)


def sample_road(city, urban, hotspot, rng):
    """Return (road_type, road_name, junction_type, speed_limit_kmh)."""
    setting = "Urban" if urban else "Rural"
    if hotspot is not None:                        # hotspots are fixed junctions
        road_type = hotspot["road_type"]
        road_name = hotspot["road_name"]
        junction = hotspot["junction_type"]
    else:
        road_type = pick(rng, cfg.ROAD_TYPE_WEIGHTS[setting])
        road_name = make_road_name(city, road_type, rng)
        junction = sample_junction(road_type, urban, rng)
    limits, weights = cfg.SPEED_LIMITS[(road_type, setting)]
    speed_limit = rng.choices(limits, weights=weights, k=1)[0]
    return road_type, road_name, junction, speed_limit


def sample_road_lighting(urban, road_type, rng):
    """Urban roads are mostly lit; rural roads mostly are not."""
    if urban:
        return pick(rng, cfg.ROAD_LIGHTING_WEIGHTS["Urban"])
    if road_type == "Motorway":
        return pick(rng, cfg.ROAD_LIGHTING_WEIGHTS["RuralMotorway"])
    return pick(rng, cfg.ROAD_LIGHTING_WEIGHTS["Rural"])


# =====================================================================
# 4. WEATHER AND ROAD SURFACE
# =====================================================================
def sample_precipitation(weather, rng):
    """Rain intensity in mm per hour. Dry weather types have 0."""
    if weather == "Rain":
        return round(min(10.0, 0.3 + rng.expovariate(1 / 1.8)), 1)
    if weather == "Heavy Rain":
        return round(min(45.0, 7.0 + rng.expovariate(1 / 6.0)), 1)
    if weather == "Snow":
        return round(rng.uniform(0.5, 5.0), 1)
    return 0.0


def sample_weather_details(weather, day_temp, hour, rng):
    """
    Turn the weather TYPE into numbers that agree with it:
    fog -> very low visibility and ~95% humidity, heavy rain -> lots of precipitation,
    windy -> strong wind, snow -> freezing temperature, and so on.
    """
    profile = cfg.WEATHER_PROFILES[weather]

    # Temperature: day average + a daily cycle (coolest ~03:00, warmest ~15:00).
    temp = day_temp + 3.0 * math.cos(2 * math.pi * (hour - 15) / 24) + rng.gauss(0, 0.7)
    if weather == "Snow":
        temp = min(temp, 1.8)
    temp = clamp(temp, -12.0, 36.0)

    humidity_mean, humidity_sd = profile["humidity"]
    humidity = clamp(rng.gauss(humidity_mean, humidity_sd) - 0.6 * (temp - 10),
                     profile["humidity_floor"], 100.0)

    low, high, mode = profile["visibility"]
    visibility = rng.triangular(low, high, mode)

    wind_mean, wind_sd = profile["wind"]
    wind = max(profile["wind_min"], rng.gauss(wind_mean, wind_sd))

    return {
        "temperature": round(temp, 1),
        "humidity": round(humidity, 1),
        "visibility": round(visibility, 2),
        "wind": round(wind, 1),
        "precipitation": sample_precipitation(weather, rng),
    }


def sample_surface(weather, temp, urban, rng):
    """Road surface follows the weather: rain -> wet, cold -> icy, snow -> snowy."""
    if weather == "Snow":
        weights = {"Snowy": 70, "Icy": 18, "Wet": 12}
    elif weather == "Heavy Rain":
        weights = {"Wet": 95, "Muddy": 2, "Dry": 3}
    elif weather == "Rain":
        weights = {"Wet": 90, "Muddy": 3, "Dry": 7}
    elif weather == "Fog":
        weights = ({"Icy": 35, "Wet": 25, "Dry": 40} if temp <= 1
                   else {"Wet": 45, "Dry": 53, "Muddy": 2})
    else:                                           # Clear, Cloudy, Windy
        if temp <= 0:
            weights = {"Icy": 18, "Wet": 14, "Dry": 68}
        elif temp <= 3:
            weights = {"Icy": 6, "Wet": 12, "Dry": 82}
        else:
            weights = {"Dry": 91, "Wet": 7, "Muddy": 2}
    if "Muddy" in weights:                          # mud is a country-road problem
        weights["Muddy"] *= 0.2 if urban else 2.0
    return pick(rng, weights)


def sample_hazard(weather, surface, urban, road_type, rng):
    """About 10% of accidents have a road hazard; the type depends on weather and place."""
    weights = {"No Hazard": 90, "Pothole": 2.0, "Debris on Road": 1.5,
               "Oil/Diesel Spill": 0.8, "Stationary Vehicle": 2.0}
    if road_type == "Motorway":
        weights["Pothole"] *= 0.3
    if not urban and road_type != "Motorway":
        weights["Animal on Road"] = 1.5
    if weather == "Rain":
        weights["Flooding"] = 0.8
    elif weather == "Heavy Rain":
        weights["Flooding"] = 2.0
        weights["Fallen Tree"] = 0.4
    elif weather == "Windy":
        weights["Fallen Tree"] = 1.5
    if surface == "Icy":
        weights["Ice Patch"] = 3.0
    return pick(rng, weights)


# =====================================================================
# 5. CONTEXT: light and traffic
# =====================================================================
def light_condition(lat, lon, day_of_year, hour_float, road_lighting):
    """
    Work out daylight / dusk / darkness from the sun position (simple astronomy):
    long summer days in Scotland, short winter days everywhere.
    """
    declination = 23.44 * math.sin(2 * math.pi * (day_of_year - 81) / 365)
    x = -math.tan(math.radians(lat)) * math.tan(math.radians(declination))
    half_day_hours = math.degrees(math.acos(clamp(x, -1.0, 1.0))) / 15.0
    solar_noon = 12.0 - lon / 15.0                       # west = later sun
    summer_time = 1.0 if 90 <= day_of_year <= 300 else 0.0  # British Summer Time
    sunrise = solar_noon - half_day_hours + summer_time
    sunset = solar_noon + half_day_hours + summer_time

    if sunrise + 0.5 <= hour_float <= sunset - 0.5:
        return "Daylight"
    if sunrise - 0.5 <= hour_float < sunrise + 0.5 or sunset - 0.5 < hour_float <= sunset + 0.5:
        return "Dawn/Dusk"
    if road_lighting == "Present - Working":
        return "Darkness - Lights Lit"
    return "Darkness - No Lighting"


def sample_traffic(hour, is_weekend, urban, weather, rng):
    """Traffic density: heavy in urban rush hours, light at night."""
    if hour >= 22 or hour <= 5:
        period = "night"
    elif not is_weekend and (7 <= hour <= 9 or 16 <= hour <= 18):
        period = "rush"
    else:
        period = "day"
    weights = list(cfg.TRAFFIC_WEIGHTS[(period, "Urban" if urban else "Rural")])
    if weather in ("Rain", "Heavy Rain", "Snow", "Fog"):     # bad weather slows traffic
        weights[2] *= 1.3
        weights[3] *= 1.3
    return rng.choices(cfg.TRAFFIC_CONDITIONS, weights=weights, k=1)[0]


# =====================================================================
# 6. WHO IS INVOLVED: pedestrians, number of vehicles, vehicle types
# =====================================================================
def sample_pedestrians(acc, rng):
    """Number of pedestrians hit (0, 1 or 2). Common in towns, rare on motorways."""
    if acc["road_type"] == "Motorway":
        probability = 0.003
    elif acc["urban_or_rural"] == "Urban":
        probability = 0.10
    else:
        probability = 0.02
    if acc["light_conditions"].startswith("Darkness"):
        probability *= 1.5
    if not chance(rng, probability):
        return 0
    return 1 if chance(rng, 0.92) else 2


def sample_number_of_vehicles(acc, pedestrian_involved, rng):
    """Mostly 1-2 vehicles; pile-ups (3+) are likelier on motorways, in fog and in heavy traffic."""
    if pedestrian_involved:
        return pick(rng, {1: 84, 2: 14, 3: 2})
    weights = dict(cfg.VEHICLE_COUNT_WEIGHTS)
    pile_up_risk = (acc["road_type"] == "Motorway" or acc["weather_condition"] == "Fog"
                    or acc["traffic_conditions"] in ("Heavy", "Congested"))
    if pile_up_risk:
        for count in weights:
            if count >= 3:
                weights[count] *= 1.8
    if acc["urban_or_rural"] == "Rural":            # single-vehicle crashes are common in the country
        weights[1] *= 1.25
    return pick(rng, weights)


def vehicle_type_weights(acc):
    """Vehicle mix changes with place, season, time of day and weather."""
    w = dict(cfg.VEHICLE_TYPE_BASE)
    if acc["road_type"] == "Motorway":
        w["Bicycle"] = 0
        w["Motorcycle"] *= 0.7
        w["Truck"] *= 3.0
        w["Bus"] *= 1.2
        w["Van"] *= 1.2
    elif acc["urban_or_rural"] == "Urban":
        w["Bus"] *= 1.5
        w["Bicycle"] *= 1.5
        w["Truck"] *= 0.7
    else:
        w["Motorcycle"] *= 1.2
        w["Truck"] *= 1.2
        w["Bicycle"] *= 0.6
        w["Bus"] *= 0.5

    month, hour = acc["month"], acc["_hour"]
    if 5 <= month <= 9:                              # riding season
        w["Motorcycle"] *= 1.8
        w["Bicycle"] *= 1.7
    elif month in (11, 12, 1, 2):
        w["Motorcycle"] *= 0.5
        w["Bicycle"] *= 0.6
    if not acc["_weekend"] and (7 <= hour <= 9 or 16 <= hour <= 18):   # commuting
        w["Bicycle"] *= 1.4
        w["Bus"] *= 1.3
    if hour >= 22 or hour <= 5:                      # night
        w["Bicycle"] *= 0.4
        w["Bus"] *= 0.4
        w["Truck"] *= 1.2
    if acc["_weekend"]:
        w["Motorcycle"] *= 1.4
        w["Truck"] *= 0.5
        w["Bus"] *= 0.7
        w["Van"] *= 0.7
    if acc["weather_condition"] in ("Rain", "Heavy Rain"):
        w["Bicycle"] *= 0.5
        w["Motorcycle"] *= 0.5
    elif acc["weather_condition"] == "Snow":
        w["Bicycle"] *= 0.3
        w["Motorcycle"] *= 0.3
    return w


def sample_vehicle_types(n_vehicles, acc, rng):
    """Pick a type for each vehicle. Cyclists rarely crash into other cyclists."""
    base = vehicle_type_weights(acc)
    types = []
    for i in range(n_vehicles):
        weights = dict(base)
        if i > 0:
            weights["Bicycle"] *= 0.4
        types.append(pick(rng, weights))
    if n_vehicles > 1 and all(t == "Bicycle" for t in types):
        types[1] = "Car"                              # make it a bike-vs-car crash
    return types


# =====================================================================
# 7. CONTRIBUTING FACTORS
# =====================================================================
def sample_contributing_factors(acc, vehicle_types, rng):
    """
    Decide which factors contributed. Several can be True at the same time.
    The probabilities depend on time, place, weather and road:

    * drunk driving   -> mostly at night and on weekends
    * speeding        -> more at night / on open roads, rare in heavy traffic
    * distracted      -> more in towns and in traffic
    * fatigue         -> small hours of the night, post-lunch dip, long open roads
    * reckless        -> much more likely together with speeding or drink
    * poor road       -> icy / snowy / muddy surface, hazards, road works
    * weather related -> only in adverse weather
    """
    hour, dow = acc["_hour"], acc["_dow"]
    night = hour >= 22 or hour <= 5
    open_road = acc["road_type"] == "Motorway" or acc["urban_or_rural"] == "Rural"
    slow_traffic = acc["traffic_conditions"] in ("Heavy", "Congested")

    # --- drink / drug driving ---
    p = 0.035
    if hour >= 22 or hour <= 4:
        p *= 5
    elif 19 <= hour <= 21:
        p *= 2
    elif 10 <= hour <= 17:
        p *= 0.3
    if acc["_weekend"] or (dow == 4 and hour >= 20):
        p *= 1.6
    drunk = chance(rng, min(p, 0.45))

    # --- speeding ---
    p = 0.14
    if night:
        p *= 1.7
    if open_road:
        p *= 1.3
    if slow_traffic:
        p *= 0.35
    speeding = chance(rng, min(p, 0.6))

    # --- distraction ---
    p = 0.17
    if acc["urban_or_rural"] == "Urban":
        p *= 1.15
    if slow_traffic:
        p *= 1.2
    if night:
        p *= 0.85
    distracted = chance(rng, min(p, 0.5))

    # --- fatigue ---
    p = 0.035
    if 0 <= hour <= 5:
        p *= 5
    elif hour >= 22:
        p *= 2.5
    elif 13 <= hour <= 15:
        p *= 1.5
    if open_road:
        p *= 1.6
    fatigue = chance(rng, min(p, 0.4))

    # --- make the flags realistic for the actual vehicles involved ---
    if all(t == "Bicycle" for t in vehicle_types):
        speeding = False                              # cyclists are not "speeding" here
    driver_states = [drunk, fatigue, distracted]      # one state per driver, in priority order
    kept = 0
    for index, active in enumerate(driver_states):
        if active:
            kept += 1
            if kept > len(vehicle_types):             # more states than drivers: drop the lowest
                driver_states[index] = False
    drunk, fatigue, distracted = driver_states

    # --- reckless driving is linked to speeding and drink ---
    if speeding:
        p = 0.30
    elif drunk:
        p = 0.20
    else:
        p = 0.04
    reckless = chance(rng, p)

    # --- road condition ---
    if acc["road_surface_condition"] in ("Icy", "Snowy", "Muddy"):
        p = 0.50
    elif acc["road_hazards"] not in ("No Hazard", "Stationary Vehicle", "Animal on Road"):
        p = 0.55
    elif acc["road_works"]:
        p = 0.25
    elif acc["road_surface_condition"] == "Wet":
        p = 0.06
    else:
        p = 0.02
    poor_road = chance(rng, p)

    # --- weather ---
    weather = acc["weather_condition"]
    if weather in ("Heavy Rain", "Snow"):
        p = 0.80
    elif weather == "Fog":
        p = 0.90 if acc["visibility_km"] < 0.2 else 0.75
    elif weather == "Windy":
        p = 0.50
    elif weather == "Rain":
        p = 0.40
    else:
        p = 0.0                                       # clear/cloudy cannot be "weather related"
    weather_related = chance(rng, p)

    # --- traffic violation (red light, illegal turn, ...) ---
    p = 0.08
    if drunk:
        p = max(p, 0.90)
    if reckless:
        p = max(p, 0.80)
    if speeding:
        p = max(p, 0.75)
    if distracted:
        p = max(p, 0.30)
    violation = chance(rng, p)

    return {
        "speeding_involved": speeding, "distracted_driving": distracted,
        "drunk_driving": drunk, "reckless_driving": reckless,
        "poor_road_condition": poor_road, "weather_related": weather_related,
        "traffic_violation": violation, "driver_fatigue": fatigue,
    }


def choose_primary_cause(flags, pedestrian_involved, has_cyclist, rural, rng):
    """
    Pick ONE main cause. If some factors are True we choose among them (drink and
    speeding are the most likely to be named). If none is True the cause is one of the
    "ordinary" causes below.
    """
    active = {}
    for flag, (label, weight) in cfg.FLAG_TO_CAUSE.items():
        if flags[flag]:
            active[label] = weight
    if active:
        return pick(rng, active)
    return pick(rng, {
        "Failed to Look Properly": 38,
        "Misjudged Distance or Speed": 30,
        "Mechanical Failure": 12,
        "Pedestrian/Cyclist Error": 25 if (pedestrian_involved or has_cyclist) else 0,
        "Animal on Road": 3 if rural else 0,
        "Undetermined": 7,
    })


# =====================================================================
# 8. VEHICLES AND DRIVERS
# =====================================================================
def sample_maneuver(vehicle_type, acc, is_primary, is_speeding, pedestrian_case, rng):
    """What the vehicle was doing: turning at junctions, overtaking when speeding, etc."""
    if is_speeding:
        return pick(rng, {"Going Ahead": 65, "Overtaking": 20, "Changing Lane": 15})
    if pedestrian_case and is_primary:
        return pick(rng, {"Going Ahead": 70, "Turning Left": 10, "Turning Right": 10,
                          "Reversing": 5, "Slowing/Stopping": 5})
    if acc["road_type"] == "Motorway":
        return pick(rng, {"Going Ahead": 70, "Changing Lane": 14, "Overtaking": 8,
                          "Slowing/Stopping": 8})
    weights = dict(cfg.BASE_MANEUVER_WEIGHTS)
    if acc["junction_type"] != "No Junction":               # junctions mean turning and waiting
        for name in ("Turning Left", "Turning Right", "Waiting to Turn"):
            weights[name] *= 3
        weights["Going Ahead"] *= 0.8
    if acc["urban_or_rural"] == "Rural":
        weights["Parked"] *= 0.2
    if vehicle_type == "Bicycle":
        weights["Reversing"] = 0
        weights["Parked"] = 0
    if is_primary:                                   # the main vehicle is moving
        weights["Parked"] = 0
    return pick(rng, weights)


def sample_vehicle_speed(vehicle_type, maneuver, speed_limit, traffic, speeding, rng):
    """
    Speed (km/h) just before the collision.
    * normal driving: 55-100% of the limit, scaled down by traffic density
    * speeding vehicles: 112-155% of the limit
    * parked / waiting / reversing vehicles are (almost) stationary
    * bicycles travel 8-32 km/h regardless of the limit
    """
    if maneuver == "Parked":
        return 0
    if maneuver == "Waiting to Turn":
        return rng.randint(0, 5)
    if maneuver == "Reversing":
        return rng.randint(2, 10)
    if vehicle_type == "Bicycle":
        return int(rng.triangular(8, 32, 18))

    if speeding:
        speed = speed_limit * rng.uniform(1.12, 1.55)
        cap = {"Car": 190, "Van": 150, "Motorcycle": 200, "Truck": 125, "Bus": 120}[vehicle_type]
        return int(min(speed, cap))

    speed = speed_limit * rng.triangular(0.55, 1.0, 0.85) * cfg.TRAFFIC_SPEED_FACTOR[traffic]
    if maneuver in ("Turning Left", "Turning Right"):
        speed *= 0.4
    elif maneuver == "Slowing/Stopping":
        speed *= 0.45
    elif maneuver == "U-Turn":
        speed *= 0.25
    elif maneuver == "Overtaking":
        speed *= 1.1
    elif maneuver == "Changing Lane":
        speed *= 0.95
    if vehicle_type in ("Truck", "Bus"):
        speed = min(speed, 96)
    return int(min(speed, speed_limit))              # not speeding -> never above the limit


def sample_age_from_bands(bands, rng, young_risk=False, distracted=False, fatigued=False):
    """Pick an age from (low, high, weight) bands, optionally tilting towards risky groups."""
    weights = []
    for low, _, weight in bands:
        if young_risk:                                # young drivers dominate speeding/drink crashes
            if low < 25:
                weight *= 2.2
            elif low < 35:
                weight *= 1.4
            elif low >= 65:
                weight *= 0.4
        if distracted and low < 35:
            weight *= 1.5
        if fatigued and 35 <= low < 65:
            weight *= 1.3
        weights.append(weight)
    low, high, _ = rng.choices(bands, weights=weights, k=1)[0]
    return rng.randint(low, high)


def driver_experience(age, vehicle_type, rng):
    """Years of driving = age minus the age the person started (minimum age + a few years)."""
    start_age = cfg.MIN_LICENCE_AGE[vehicle_type] + int(rng.expovariate(1 / 3.0))
    return max(0, age - start_age)


def build_vehicles(acc, types, flags, pedestrian_case, rng, vehicle_counter):
    """Create the vehicle + driver rows of one accident (damage is added later)."""
    n = len(types)
    night = acc["_hour"] >= 22 or acc["_hour"] <= 5

    # Which driver is in which state? (drink first, then fatigue, then distraction)
    conditions = ["Normal"] * n
    states = []
    if flags["drunk_driving"]:
        states.append("Alcohol Impaired" if chance(rng, 0.85) else "Drug Impaired")
    if flags["driver_fatigue"]:
        states.append("Fatigued")
    if flags["distracted_driving"]:
        states.append("Distracted")
    for i, state in enumerate(states):
        conditions[i] = state

    # The speeding vehicle is the first motorised one; vehicle 1 is the "main" vehicle.
    speeding_index = None
    if flags["speeding_involved"]:
        speeding_index = next(i for i, t in enumerate(types) if t != "Bicycle")
    risky_index = speeding_index if speeding_index is not None else 0
    risky_accident = (flags["speeding_involved"] or flags["reckless_driving"]
                      or flags["drunk_driving"])

    vehicles = []
    for i, vtype in enumerate(types):
        is_speeding = (i == speeding_index)
        maneuver = sample_maneuver(vtype, acc, i == 0, is_speeding, pedestrian_case, rng)
        speed = sample_vehicle_speed(vtype, maneuver, acc["speed_limit"],
                                     acc["traffic_conditions"], is_speeding, rng)
        condition = conditions[i]

        # --- vehicle ---
        make, model, cc = rng.choice(cfg.VEHICLE_CATALOG[vtype])
        age_years = min(cfg.VEHICLE_MAX_AGE[vtype],
                        int(rng.gammavariate(2.0, cfg.VEHICLE_MEAN_AGE[vtype] / 2.0)))

        # --- driver ---
        unknown_driver = chance(rng, cfg.UNKNOWN_DRIVER_RATE)
        if unknown_driver:
            driver_age = gender = experience = None
        else:
            young_risk = risky_accident and i == risky_index
            driver_age = sample_age_from_bands(cfg.DRIVER_AGE_BANDS[vtype], rng,
                                               young_risk=young_risk,
                                               distracted=(condition == "Distracted"),
                                               fatigued=(condition == "Fatigued"))
            male_share = cfg.MALE_SHARE[vtype]
            if young_risk:                            # fewer women among risk-taking drivers
                male_share = 1 - (1 - male_share) * 0.6
            gender = "Male" if chance(rng, male_share) else "Female"
            experience = driver_experience(driver_age, vtype, rng)

        # --- safety equipment (None = not applicable for this vehicle) ---
        seatbelt = helmet = None
        if vtype in cfg.SEATBELT_VEHICLES:
            p_no_belt = 0.05
            if condition in ("Alcohol Impaired", "Drug Impaired"):
                p_no_belt *= 3
            if night:
                p_no_belt *= 1.4
            if driver_age is not None and driver_age < 25:
                p_no_belt *= 1.3
            seatbelt = not chance(rng, min(p_no_belt, 0.5))
        elif vtype in cfg.HELMET_VEHICLES:
            p_helmet = 0.96 if vtype == "Motorcycle" else 0.38
            if condition in ("Alcohol Impaired", "Drug Impaired"):
                p_helmet *= 0.7
            helmet = chance(rng, p_helmet)

        vehicles.append({
            "vehicle_id": f"VEH{next(vehicle_counter):08d}",
            "accident_id": acc["accident_id"],
            "vehicle_reference": i + 1,
            "vehicle_type": vtype,
            "vehicle_make": make,
            "vehicle_model": model,
            "vehicle_year": acc["year"] - age_years,
            "vehicle_age": age_years,
            "vehicle_speed_kmh": speed,
            "vehicle_direction": rng.choices(cfg.VEHICLE_DIRECTIONS,
                                             weights=cfg.VEHICLE_DIRECTION_WEIGHTS, k=1)[0],
            "vehicle_maneuver": maneuver,
            "vehicle_damage_severity": None,          # filled in after the severity is known
            "driver_age": driver_age,
            "driver_gender": gender,
            "driver_experience_years": experience,
            "seatbelt_used": seatbelt,
            "helmet_used": helmet,
            "driver_condition": condition,
            "vehicle_color": None if chance(rng, cfg.MISSING_COLOR_RATE) else pick(rng, cfg.VEHICLE_COLORS),
            "engine_capacity_cc": cc,
        })
    return vehicles


# =====================================================================
# 9. ACCIDENT SEVERITY AND VEHICLE DAMAGE
# =====================================================================
def sample_severity(acc, flags, vehicles, n_pedestrians, rng):
    """
    Build a "risk score" and convert it to Slight / Serious / Fatal with two logistic
    models. Higher scores mean worse outcomes. Contributions (higher = more severe):

    speeding, drink, reckless, fatigue, high speed limit and high vehicle speed,
    darkness (especially unlit roads), rural roads, pedestrians, motorcycles, bicycles,
    trucks, unbelted occupants, missing helmets, fog and multi-vehicle pile-ups.
    """
    score = 0.0
    score += 0.90 * flags["speeding_involved"] + 0.80 * flags["drunk_driving"]
    score += 0.45 * flags["reckless_driving"] + 0.35 * flags["driver_fatigue"]
    score += 0.15 * flags["distracted_driving"]

    score += (acc["speed_limit"] - 64) / 48 * 0.7
    max_speed = max(v["vehicle_speed_kmh"] for v in vehicles)
    score += (max_speed - 55) / 50 * 0.6

    light = acc["light_conditions"]
    if light == "Darkness - No Lighting":
        score += 0.50
    elif light == "Darkness - Lights Lit":
        score += 0.15
    elif light == "Dawn/Dusk":
        score += 0.05
    if acc["urban_or_rural"] == "Rural":
        score += 0.35
    if n_pedestrians:
        score += 1.0

    types = [v["vehicle_type"] for v in vehicles]
    if "Motorcycle" in types:
        score += 0.70
    if "Bicycle" in types:
        score += 0.50
    if "Truck" in types:
        score += 0.40
    if "Bus" in types:
        score += 0.15
    if any(v["seatbelt_used"] is False for v in vehicles):
        score += 0.35
    if any(v["helmet_used"] is False and v["vehicle_type"] == "Motorcycle" for v in vehicles):
        score += 0.60
    if any(v["helmet_used"] is False and v["vehicle_type"] == "Bicycle" for v in vehicles):
        score += 0.20
    if len(vehicles) >= 3:
        score += 0.25

    weather = acc["weather_condition"]
    score += {"Fog": 0.25, "Heavy Rain": 0.10, "Snow": -0.20, "Windy": 0.05}.get(weather, 0.0)

    if not chance(rng, sigmoid(cfg.SEVERITY_INTERCEPT_SERIOUS + score)):
        return "Slight"
    if chance(rng, sigmoid(cfg.SEVERITY_INTERCEPT_FATAL + 0.8 * score)):
        return "Fatal"
    return "Serious"


def damage_level(vehicle, all_vehicles, accident_severity, rng):
    """Vehicle damage grows with impact speed and with the accident severity."""
    own = vehicle["vehicle_speed_kmh"]
    others = [v["vehicle_speed_kmh"] for v in all_vehicles if v is not vehicle]
    impact_speed = max(own, 0.6 * max(others)) if others else own   # parked cars get hit too
    score = (impact_speed / 30
             + {"Slight": 0.0, "Serious": 1.0, "Fatal": 2.0}[accident_severity]
             + cfg.DAMAGE_TYPE_ADJUST[vehicle["vehicle_type"]]
             + rng.gauss(0, 0.8))
    for level, limit in zip(cfg.DAMAGE_LEVELS, (0.9, 2.2, 3.6, 5.0)):
        if score < limit:
            return level
    return "Total Loss"


def sample_duration(acc, severity, n_vehicles, rng):
    """Minutes until the road is clear: longer for severe, motorway and multi-vehicle accidents."""
    median = {"Slight": 35, "Serious": 90, "Fatal": 240}[severity]
    minutes = rng.lognormvariate(math.log(median), 0.5)
    if acc["road_type"] == "Motorway":
        minutes *= 1.5
    minutes *= 1 + 0.15 * (n_vehicles - 1)
    if acc["weather_condition"] in ("Heavy Rain", "Snow", "Fog"):
        minutes *= 1.2
    return int(clamp(round(minutes), 10, 720))


# =====================================================================
# 10. CASUALTIES
# =====================================================================
def vulnerability(casualty):
    """How easily this person is badly hurt (pedestrians, riders, unbelted, elderly = higher)."""
    v = {"Pedestrian": 2.2, "Motorcyclist": 2.4, "Cyclist": 2.0}.get(casualty["casualty_type"], 1.0)
    if casualty["seatbelt_used"] is False:
        v *= 1.8
    if casualty["helmet_used"] is False:
        v *= 1.7
    if casualty["age"] is not None and casualty["age"] >= 70:
        v *= 1.5
    return v


def assign_injury_severity(casualties, accident_severity, rng):
    """
    Give each casualty an injury severity that agrees with the accident severity:
    * Slight accident  -> everybody Slight.
    * Serious accident -> at least one Serious, the others Slight or Serious.
    * Fatal accident   -> at least one Fatal, the others Fatal / Serious / Slight.
    The most vulnerable person is the most likely to be the worst-injured one.
    """
    if accident_severity == "Slight":
        for c in casualties:
            c["injury_severity"] = "Slight"
        return

    worst = rng.choices(casualties, weights=[vulnerability(c) for c in casualties], k=1)[0]
    for c in casualties:
        if c is worst:
            c["injury_severity"] = accident_severity
            continue
        v = vulnerability(c)
        if accident_severity == "Fatal":
            p_fatal = min(0.30, 0.07 * v)
            p_serious = min(0.55, 0.30 * v)
            r = rng.random()
            if r < p_fatal:
                c["injury_severity"] = "Fatal"
            elif r < p_fatal + p_serious:
                c["injury_severity"] = "Serious"
            else:
                c["injury_severity"] = "Slight"
        else:
            c["injury_severity"] = "Serious" if chance(rng, min(0.60, 0.20 * v)) else "Slight"


def sample_passenger_count(vehicle_type, rng):
    """How many passengers travel with the driver (buses carry many)."""
    if vehicle_type == "Bus":
        return 0 if chance(rng, 0.1) else rng.randint(3, 25)
    return pick(rng, cfg.PASSENGER_COUNT_WEIGHTS[vehicle_type])


def weighted_sample_without_replacement(items, k, rng):
    """Pick k different items; items with a higher "w" are more likely to be picked."""
    items = list(items)
    chosen = []
    for _ in range(min(k, len(items))):
        index = rng.choices(range(len(items)), weights=[it["w"] for it in items], k=1)[0]
        chosen.append(items.pop(index))
    return chosen


def build_casualties(acc, accident_severity, vehicles, n_pedestrians, rng, casualty_counter):
    """
    Create the casualty rows of one accident.

    1. Build a pool of everybody who COULD be hurt: each driver/rider, their passengers
       and the pedestrians (a pedestrian accident always has the pedestrian as casualty).
    2. Decide how many people were hurt (depends on severity, vehicles, buses).
    3. Pick that many people (riders and pedestrians are the likeliest to be hurt).
    4. Give each person age, gender, safety equipment and an injury severity.
    """
    pool = []
    for v in vehicles:
        role = {"Motorcycle": "Motorcyclist", "Bicycle": "Cyclist"}.get(v["vehicle_type"], "Driver")
        pool.append({"casualty_type": role, "vehicle": v, "w": cfg.ROLE_PICK_WEIGHT[role]})
        for _ in range(sample_passenger_count(v["vehicle_type"], rng)):
            pool.append({"casualty_type": "Passenger", "vehicle": v,
                         "w": cfg.ROLE_PICK_WEIGHT["Passenger"]})

    target = pick(rng, cfg.CASUALTY_COUNT_WEIGHTS[accident_severity])
    target += sum(1 for _ in range(len(vehicles) - 1) if chance(rng, 0.25))   # more vehicles, more injured
    if any(v["vehicle_type"] == "Bus" for v in vehicles):
        target += rng.randint(0, 3)
    wanted_from_pool = max(target - n_pedestrians, 0 if n_pedestrians else 1)

    people = [{"casualty_type": "Pedestrian", "vehicle": None, "w": 0} for _ in range(n_pedestrians)]
    people += weighted_sample_without_replacement(pool, wanted_from_pool, rng)

    casualties = []
    for person in people:
        vehicle, ctype = person["vehicle"], person["casualty_type"]
        seatbelt = helmet = None

        if ctype in ("Driver", "Motorcyclist", "Cyclist"):      # same person as the vehicle's driver
            age, gender = vehicle["driver_age"], vehicle["driver_gender"]
            seatbelt, helmet = vehicle["seatbelt_used"], vehicle["helmet_used"]
        elif ctype == "Passenger":
            age = sample_age_from_bands(cfg.PASSENGER_AGE_BANDS, rng)
            gender = "Female" if chance(rng, 0.52) else "Male"
            if vehicle["vehicle_type"] in cfg.SEATBELT_VEHICLES:   # passengers copy the driver's habits
                seatbelt = chance(rng, 0.95 if vehicle["seatbelt_used"] else 0.40)
            elif vehicle["vehicle_type"] == "Motorcycle":          # pillion passenger
                helmet = chance(rng, 0.95 if vehicle["helmet_used"] else 0.15)
        else:                                                      # Pedestrian
            age = sample_age_from_bands(cfg.PEDESTRIAN_AGE_BANDS, rng)
            gender = "Female" if chance(rng, 0.50) else "Male"

        if ctype in ("Passenger", "Pedestrian") and chance(rng, cfg.MISSING_CASUALTY_AGE_RATE):
            age = None                                             # age not recorded

        casualties.append({
            "casualty_id": None,                                   # assigned below (after the sort)
            "accident_id": acc["accident_id"],
            "vehicle_id": vehicle["vehicle_id"] if vehicle else None,
            "casualty_type": ctype,
            "age": age,
            "gender": gender,
            "injury_severity": None,
            "hospitalized": False,
            "emergency_response_required": False,
            "seatbelt_used": seatbelt,
            "helmet_used": helmet,
        })

    assign_injury_severity(casualties, accident_severity, rng)

    for c in casualties:
        severity = c["injury_severity"]
        vulnerable = c["casualty_type"] in ("Pedestrian", "Motorcyclist", "Cyclist")
        if severity == "Fatal":
            c["hospitalized"] = chance(rng, 0.70)                  # others died at the scene
            c["emergency_response_required"] = True
        elif severity == "Serious":
            c["hospitalized"] = True
            c["emergency_response_required"] = chance(rng, 0.98)
        else:
            c["hospitalized"] = chance(rng, 0.15 if vulnerable else 0.08)
            c["emergency_response_required"] = (chance(rng, 0.85) if c["hospitalized"]
                                                else chance(rng, 0.25))
        c["casualty_id"] = f"CAS{next(casualty_counter):08d}"
    return casualties


# =====================================================================
# 11. PUTTING ONE ACCIDENT TOGETHER
# =====================================================================
def maybe_missing(value, rng, rate):
    """Return None (missing) with a small probability, otherwise the value."""
    return None if chance(rng, rate) else value


def build_one_accident(ctx, rng, vehicle_counter, casualty_counter):
    """
    The "recipe" for one accident. `ctx` is one row of the timeline
    (date, time, city, weather type). Returns (accident_row, vehicle_rows, casualty_rows).
    """
    city = cfg.CITIES[int(ctx.city_idx)]
    when = ctx.date
    hour, minute, second = int(ctx.hour), int(ctx.minute), int(ctx.second)
    dow = when.dayofweek
    weekend = dow >= 5

    # --- Steps 2-3: location and road ---
    urban = chance(rng, cfg.URBAN_SHARE)
    area, lat, lon, hotspot = sample_location(city, urban, rng)
    road_type, road_name, junction, speed_limit = sample_road(city, urban, hotspot, rng)
    road_lighting = sample_road_lighting(urban, road_type, rng)
    setting = "Motorway" if road_type == "Motorway" else ("Urban" if urban else "Rural")
    road_works = chance(rng, cfg.ROAD_WORKS_PROBABILITY[setting])

    # --- Step 4: weather (fog often lifts around midday) ---
    weather = ctx.weather
    if weather == "Fog" and 11 <= hour <= 16 and chance(rng, 0.6):
        weather = "Cloudy"
    w = sample_weather_details(weather, ctx.day_temp, hour, rng)
    surface = sample_surface(weather, w["temperature"], urban, rng)
    hazard = sample_hazard(weather, surface, urban, road_type, rng)

    # --- Step 5: light and traffic ---
    light = light_condition(lat, lon, when.dayofyear, hour + minute / 60, road_lighting)
    traffic = sample_traffic(hour, weekend, urban, weather, rng)

    # Keys starting with "_" are helper values and are NOT written to the CSV.
    acc = {
        "accident_id": ctx.accident_id,
        "accident_date": when.strftime("%Y-%m-%d"),
        "accident_time": f"{hour:02d}:{minute:02d}:{second:02d}",
        "day_of_week": when.day_name(),
        "month": when.month,
        "year": when.year,
        "country": cfg.COUNTRY,
        "city": city["name"],
        "area": area,
        "latitude": lat,
        "longitude": lon,
        "road_name": road_name,
        "road_type": road_type,
        "urban_or_rural": "Urban" if urban else "Rural",
        "junction_type": junction,
        "speed_limit": speed_limit,
        "traffic_conditions": traffic,
        "light_conditions": light,
        "weather_condition": weather,
        "temperature_celsius": w["temperature"],
        "humidity_percentage": w["humidity"],
        "visibility_km": w["visibility"],
        "wind_speed_kmh": w["wind"],
        "precipitation_mm": w["precipitation"],
        "road_surface_condition": surface,
        "road_lighting": road_lighting,
        "road_hazards": hazard,
        "road_works": road_works,
        "_hour": hour, "_dow": dow, "_weekend": weekend,
    }

    # --- Step 6: who is involved ---
    n_pedestrians = sample_pedestrians(acc, rng)
    n_vehicles = sample_number_of_vehicles(acc, n_pedestrians > 0, rng)
    types = sample_vehicle_types(n_vehicles, acc, rng)

    # --- Step 7: contributing factors ---
    flags = sample_contributing_factors(acc, types, rng)

    # --- Step 8: vehicles and drivers ---
    vehicles = build_vehicles(acc, types, flags, n_pedestrians > 0, rng, vehicle_counter)

    # --- Step 9: severity, damage, duration ---
    severity = sample_severity(acc, flags, vehicles, n_pedestrians, rng)
    for v in vehicles:
        v["vehicle_damage_severity"] = damage_level(v, vehicles, severity, rng)

    # --- Step 10: casualties ---
    casualties = build_casualties(acc, severity, vehicles, n_pedestrians, rng, casualty_counter)

    acc.update(flags)
    acc["accident_severity"] = severity
    acc["number_of_vehicles"] = len(vehicles)
    acc["number_of_casualties"] = len(casualties)
    acc["accident_duration_minutes"] = sample_duration(acc, severity, len(vehicles), rng)
    acc["primary_cause"] = choose_primary_cause(
        flags, n_pedestrians > 0, "Bicycle" in types, not urban, rng)

    # Realistic sensor gaps (a few percent at most) are applied last so the logic above
    # always sees the true values.
    acc["temperature_celsius"] = maybe_missing(acc["temperature_celsius"], rng, cfg.MISSING_TEMPERATURE_RATE)
    for column in ("humidity_percentage", "visibility_km", "wind_speed_kmh"):
        acc[column] = maybe_missing(acc[column], rng, cfg.MISSING_WEATHER_RATE)

    return acc, vehicles, casualties


# =====================================================================
# 12. THE MAIN ENTRY POINT OF THIS MODULE
# =====================================================================
def finalize_dtypes(accidents, vehicles, casualties):
    """Use nullable pandas types so integers stay integers and 'not applicable' stays empty."""
    for column in ("driver_age", "driver_experience_years", "engine_capacity_cc"):
        vehicles[column] = vehicles[column].astype("Int64")
    for column in ("seatbelt_used", "helmet_used"):
        vehicles[column] = vehicles[column].astype("boolean")
        casualties[column] = casualties[column].astype("boolean")
    casualties["age"] = casualties["age"].astype("Int64")
    return accidents, vehicles, casualties


def generate_dataset(num_accidents, seed, show_progress=True):
    """
    Generate the three SYNTHETIC tables and return them as pandas DataFrames
    (accidents, vehicles, casualties). The work is done in chunks to keep memory small.
    """
    rng = random.Random(seed)                        # python random: used for per-accident decisions
    np_rng = np.random.default_rng(seed)             # numpy random: used for bulk sampling

    timeline = build_timeline(num_accidents, rng, np_rng)

    vehicle_counter = itertools.count(1)             # stable, sequential IDs
    casualty_counter = itertools.count(1)
    accident_frames, vehicle_frames, casualty_frames = [], [], []

    for start in range(0, num_accidents, cfg.CHUNK_SIZE):
        chunk = timeline.iloc[start:start + cfg.CHUNK_SIZE]
        accident_rows, vehicle_rows, casualty_rows = [], [], []
        for ctx in chunk.itertuples(index=False):
            accident, vehicles, casualties = build_one_accident(
                ctx, rng, vehicle_counter, casualty_counter)
            accident_rows.append(accident)
            vehicle_rows.extend(vehicles)
            casualty_rows.extend(casualties)

        accident_frames.append(pd.DataFrame(accident_rows)[cfg.ACCIDENT_COLUMNS])
        vehicle_frames.append(pd.DataFrame(vehicle_rows)[cfg.VEHICLE_COLUMNS])
        casualty_frames.append(pd.DataFrame(casualty_rows)[cfg.CASUALTY_COLUMNS])

        if show_progress:
            done = min(start + cfg.CHUNK_SIZE, num_accidents)
            print(f"  generated {done:,} / {num_accidents:,} accidents")

    accidents = pd.concat(accident_frames, ignore_index=True)
    vehicles = pd.concat(vehicle_frames, ignore_index=True)
    casualties = pd.concat(casualty_frames, ignore_index=True)
    return finalize_dtypes(accidents, vehicles, casualties)
