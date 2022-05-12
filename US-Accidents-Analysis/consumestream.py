import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
import json
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler

def preprocess(data):
    df_columns = ['ID', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng',
       'End_Lat', 'End_Lng', 'Distance(mi)', 'Description', 'Number', 'Street',
       'Side', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone',
       'Airport_Code', 'Weather_Timestamp', 'Temperature(F)', 'Wind_Chill(F)',
       'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Direction',
       'Wind_Speed(mph)', 'Precipitation(in)', 'Weather_Condition', 'Amenity',
       'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
       'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
       'Turning_Loop', 'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
       'Astronomical_Twilight']
    df_event = pd.DataFrame([data], columns=df_columns)
    data = df_event
    irrelavant_columns = ['ID','Description','Country','Weather_Timestamp']
    data_preprocessed_df = data.drop(irrelavant_columns, axis=1)
    data_preprocessed_df.replace("", float("NaN"), inplace=True)
    data_preprocessed_df.replace(" ", float("NaN"), inplace=True)

    # Count missing value(NaN, na, null, None) of each columns, Then transform the result to a pandas dataframe. 
    count_missing_value = data_preprocessed_df.isna().sum() / data_preprocessed_df.shape[0] * 100
    count_missing_value_df = pd.DataFrame(count_missing_value.sort_values(ascending=False), columns=['Missing%'])
    missing_value_40_df = count_missing_value_df[count_missing_value_df['Missing%'] > 40]
    data_preprocessed_df.drop(missing_value_40_df.index, axis=1, inplace=True)
    numerical_missing = ['Wind_Speed(mph)', 'End_Lng', 'End_Lat', 'Visibility(mi)','Humidity(%)', 'Temperature(F)', 'Pressure(in)']
    categorical_missing = ['Weather_Condition','Wind_Direction', 'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight', 'Side']
    data_preprocessed_dropNaN_df = data_preprocessed_df.dropna()
    data_preprocessed_dropNaN_df.reset_index(drop=True, inplace=True)

    data_preprocessed_median_df = data_preprocessed_df.copy()

    # For numerical columns
    for column_name in numerical_missing:
        data_preprocessed_median_df[column_name] = data_preprocessed_median_df.groupby('Severity')[column_name].transform(lambda x:x.fillna(x.median()))

    # # For categorical columns(Majority value imputation)

    for column_name in categorical_missing:
        data_preprocessed_median_df[column_name] = data_preprocessed_median_df.groupby('Severity')[column_name].transform(lambda x:x.fillna(x.fillna(x.mode().iloc[0])))

    # Drop NaN and reset index
    data_preprocessed_median_df.dropna(inplace=True)

    data_preprocessed_mean_df = data_preprocessed_df.copy()

    # For numerical columns
    for column_name in numerical_missing:
        data_preprocessed_mean_df[column_name] = data_preprocessed_mean_df.groupby('Severity')[column_name].transform(lambda x:x.fillna(x.mean()))

    # For categorical columns(Majority value imputation)
    for column_name in categorical_missing:
        data_preprocessed_mean_df[column_name] = data_preprocessed_mean_df.groupby('Severity')[column_name].transform(lambda x:x.fillna(x.fillna(x.mode().iloc[0])))
        
    # Drop NaN 
    data_preprocessed_mean_df.dropna(inplace=True)

    data_best_df = data_preprocessed_dropNaN_df.copy()
    data_best_df.reset_index(inplace=True)

    relevant_features = ['Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng','Side',
       'Temperature(F)', 'Humidity(%)', 'Pressure(in)', 'Visibility(mi)',
       'Wind_Direction', 'Wind_Speed(mph)', 'Weather_Condition', 'Amenity',
       'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
       'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
       'Turning_Loop', 'Sunrise_Sunset']
    data_modelling_df = data_best_df[relevant_features].copy()
    one_hot_features = ['Wind_Direction', 'Weather_Condition']

# Wind_Direction Categorizing
    data_modelling_df.loc[data_modelling_df['Wind_Direction'].str.startswith('C'), 'Wind_Direction'] = 'C' #Calm
    data_modelling_df.loc[data_modelling_df['Wind_Direction'].str.startswith('E'), 'Wind_Direction'] = 'E' #East, ESE, ENE
    data_modelling_df.loc[data_modelling_df['Wind_Direction'].str.startswith('W'), 'Wind_Direction'] = 'W' #West, WSW, WNW
    data_modelling_df.loc[data_modelling_df['Wind_Direction'].str.startswith('S'), 'Wind_Direction'] = 'S' #South, SSW, SSE
    data_modelling_df.loc[data_modelling_df['Wind_Direction'].str.startswith('N'), 'Wind_Direction'] = 'N' #North, NNW, NNE
    data_modelling_df.loc[data_modelling_df['Wind_Direction'].str.startswith('V'), 'Wind_Direction'] = 'V' #Variable

    data_modelling_df['Weather_Fair'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Fair', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Cloudy'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Cloudy', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Clear'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Clear', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Overcast'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Overcast', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Snow'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Snow|Wintry|Sleet', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Haze'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Smoke|Fog|Mist|Haze', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Rain'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Rain|Drizzle|Showers', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Thunderstorm'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Thunderstorms|T-Storm', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Windy'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Windy|Squalls', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Hail'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Hail|Ice Pellets', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Thunder'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Thunder', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Dust'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Dust', case=False, na = False), 1, 0)
    data_modelling_df['Weather_Tornado'] = np.where(data_modelling_df['Weather_Condition'].str.contains('Tornado', case=False, na = False), 1, 0)

    onehot_df = pd.get_dummies(data_modelling_df['Wind_Direction'], prefix='Wind')
    data_modelling_df = pd.concat([data_modelling_df, onehot_df], axis=1)
    data_modelling_df.drop(one_hot_features, axis=1, inplace=True)

    label_encoding_features = ['Side', 'Amenity','Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway','Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal','Turning_Loop', 'Sunrise_Sunset']

    # Label Encoding
    for feature in label_encoding_features:
        data_modelling_df[feature] = LabelEncoder().fit_transform(data_modelling_df[feature])

    data_modelling_df.drop(["Start_Time","End_Time"], axis=1, inplace=True)

    return data_modelling_df


def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%M-%m-%d %H:%M:%S")
    val = msg.value()
    dval = json.loads(val)

    client = MongoClient()
    db = client['USA']

# Processing through each message received
    # for message in consumer:
    try:
        # print(message)
        events = dval
        events_list = list(events.values())
        result = preprocess(events_list[0])
        #Convert dataframe to list
        result_list = result.values.tolist()
        result = db["acciednts"].insert_one({time_start:result_list[0]})
    except Exception as e:
        print(e)
    # print(time_start, dval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()