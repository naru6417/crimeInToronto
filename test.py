import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import json

base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
url = base_url + "/api/3/action/datastore_search"
resource = {
    "id": "58b33705-45f0-4796-a1a7-5762cc152772"
}

p = {
    "id": resource["id"]
}

resource_columns = requests.get(url, params=p).json()["result"]["records"]

f = resource_columns[0]

fields = ["_id", "AREA_NAME", "HOOD_ID", "geometry"]

for key in f:
    if key.endswith("_2023"):
        fields.append(key)

# adjustable params
p = {
    "id": resource["id"],
    "limit": 5000,
    "fields": fields
}
resource_search_data = requests.get(url, params=p).json()["result"]["records"]

df = pd.DataFrame(resource_search_data)


# save to a GeoJSON or Shapefile
# df.to_csv("filtered_data.csv")

# save to geojson

def parse_geom(geom_str):
    try:
        return shape(json.loads(geom_str))
    except (TypeError, AttributeError):  # Handle NaN and empty strings
        return None


df["geometry"] = df["geometry"].apply(parse_geom)
gdf = gpd.GeoDataFrame(df, geometry='geometry')
gdf.to_file('output.geojson')
