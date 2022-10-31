import os
import requests
from dotenv import load_dotenv

# ------------------------------------------ GET ------------------------------------------
def getNearbyResults(query, location, radius=2000, limit=50):
    token_fsq = os.getenv("fs_api_key")
    ll = f"{location[1]}%2C{location[0]}"
    url = f"https://api.foursquare.com/v3/places/search?query={query}&ll={ll}&radius={str(radius)}&limit={str(limit)}&sort=DISTANCE"

    headers = {
        "accept": "application/json",
        "Authorization": token_fsq,
    }
    return requests.get(url, headers=headers).json()

def getVenue(query, location):
    token_fsq = os.getenv("fs_api_key")
    ll = f"{location[1]}%2C{location[0]}"
    url = f"https://api.foursquare.com/v3/places/search?query={query}&ll={ll}&limit=1"

    headers = {
        "accept": "application/json",
        "Authorization": token_fsq,
    }
    response = requests.get(url, headers=headers).json()
    return response

def getClosesVenue(query, location):
    token_fsq = os.getenv("fs_api_key")
    ll = f"{location[1]}%2C{location[0]}"
    url = f"https://api.foursquare.com/v3/places/search?query={query}&ll={ll}&limit=1&sort=DISTANCE"

    headers = {
        "accept": "application/json",
        "Authorization": token_fsq,
    }
    response = requests.get(url, headers=headers).json()
    return response

def addDistanceInfo(df, distance, index, column):
    try:
        true_distance = distance['results'][0]['distance']
        df.loc[index, column] = true_distance
    except:
        df.loc[index, column] = None
    return df




# ------------------------------------------ CLEAN ------------------------------------------
def responseToList(response):
    try:
        new_list = []
        for i in response['results']:
            name = i["name"]
            address =  i["location"]["formatted_address"]
            lat = i["geocodes"]["main"]["latitude"]
            lon = i["geocodes"]["main"]["longitude"]

            type_ = {"typepoint": 
                                {"type": "Point", 
                                "coordinates": [lat, lon]}}

            new_list.append({"name":name, "lat":lat, "lon":lon, "type":type_})
    except Exception as e:
        print(e)
    return new_list

def getNumberOfResponse(response):
    return len(response)