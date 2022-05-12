from flask import Flask
import requests
import folium
import folium.plugins
import json
import pandas as pd
from pymongo.mongo_client import MongoClient
app = Flask(__name__)

def get_data(no_values,collection):
    cursor = collection.find()
    newJson = []
    obj_list = []
    list_cur = list(cursor)  
    for i in range(no_values):
        newJson.append(list_cur[i])
    for i in newJson:
        new = list(i.values())
        obj_list.append(new[1:])
    return obj_list


@app.route('/')
def index():
#FOLIUM MAP'
    client = MongoClient()
    db = client['USA']
    collection = db["accidents"]
    values = get_data(10,collection)
    values_string = ','.join(str(e) for e in values[0][1:])
    prediction  = requests.post("https://r1cu3lhkl0.execute-api.us-east-1.amazonaws.com/test/predictseverity",json={"data": values_string})
    orlando_lat = 28.538336
    orlando_long = -81.379234
    startLatitude_list = [40.11652]
    startLongitude_list = [-75.34903]
    prediction_list = [prediction.text]

    # Generate a map of Orlando
    orlando_map = folium.Map(location=[orlando_lat, orlando_long], zoom_start=12)

    # Instantiate a mark cluster object for the incidents in the dataframe
    accidents = folium.plugins.MarkerCluster().add_to(orlando_map)

    # Loop through the dataframe and add each data point to the mark cluster
    for lat, lng, label in zip(startLatitude_list, startLongitude_list, prediction_list):
        label =str(int(label))
        if label == '4':
            folium.Marker(
                location=[lat, lng],
                icon=folium.Icon(color="red", icon="warning-sign"),
                popup=label,
                ).add_to(accidents)
        elif label == '3':
            folium.Marker(
                location=[lat, lng],
                icon=folium.Icon(color="lightred", icon="warning-sign"),
                popup=label,
                ).add_to(accidents)
        elif label == '2':
            folium.Marker(
                location=[lat, lng],
                icon=folium.Icon(color="orange", icon="warning-sign"),
                popup=label,
                ).add_to(accidents)
        elif label == '1':
            folium.Marker(
                location=[lat, lng],
                icon=folium.Icon(color="beige", icon="warning-sign"),
                popup=label,
                ).add_to(accidents)
# Display map
    
    return orlando_map._repr_html_()


if __name__ == '__main__':
    app.run(debug=True)