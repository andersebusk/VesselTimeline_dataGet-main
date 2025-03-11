# Import libraries
import pandas as pd
import requests
import time
from pymongo import MongoClient
import ast

# Load parameters
with open("keys.txt", "r") as f:
    auth_dict = ast.literal_eval(f.read())

# Create function for getting response
def get_vessel(vessel_IMO_number, start_date=None, date_range="P90D"):
    parameters = {"vesselIMONumber" : vessel_IMO_number,
                  "carrierCodes" : "MAEU",
                  "startDate" : start_date,
                  "dateRange" : date_range}
    headers = {"Consumer-Key": auth_dict["maersk"]["consumerkey"]}
    return requests.get("https://api.maersk.com/schedules/vessel-schedules", params=parameters, headers = headers)


# Create function for creating DF from response
def vessel_DF_creator(response):
    vesselDF = pd.DataFrame({
        "vessel_name" : [],
        "vessel_IMO" : [], 
        "port_name" : [],
        "port_UN_location_code" : [],
        "country_name" : [],
        "ARR" : [],
        "DEP" : []})
    response = response.json()
    for call in response["vesselCalls"]:
        new_row = pd.DataFrame({
            "vessel_name" : [response["vessel"].get("vesselName")],
            "vessel_IMO" : [response["vessel"].get("vesselIMONumber")],
            "port_name" : [call["facility"].get("portName")],
            "port_UN_location_code": [call["facility"].get("UNLocationCode")],
            "country_name" : [call["facility"].get("countryName")],
            "ARR" : [call["callSchedules"][0].get("classifierDateTime")],
            "DEP" : [call["callSchedules"][1].get("classifierDateTime")],
            })
        vesselDF = pd.concat([vesselDF, new_row])
    return vesselDF


# Create function for creating a dataframe from a list of vessels
def total_DF_creator(trackerDF, startDate=None, dateRange="P60D"):
    totalDF = pd.DataFrame({})
    error_list = []
    for index, vesselIMO in trackerDF["IMO"].items():
        vesselIMO = str(vesselIMO)
        if (not vesselIMO.isnumeric()) or (len(vesselIMO) != 7): # Print an error message if passed vesselIMO does not have correct format
            print("{} in row {} is not a valid IMO-number.".format(vesselIMO, index + 1))
            continue
        response = get_vessel(vesselIMO, startDate, dateRange)
        if response.status_code == 200:
            print("{} okay.".format(vesselIMO))
            newDF = vessel_DF_creator(response)
            newDF["class"] = trackerDF.loc[index, "CLASS"]
            newDF["project"] = trackerDF.loc[index, "PROJECT"]
            totalDF = pd.concat([totalDF, newDF])
            time.sleep(0.3) # Add sleep to not exceed 5 requests/second
        else:
            print("Error retrieving vessel no. {}. Error code {}".format(vesselIMO, response.status_code))
            error_list.append("IMO: {} // Error code: {}".format(vesselIMO, response.status_code))
            continue
    totalDF.reset_index(drop=True, inplace=True) # Resetting indices
    return totalDF, error_list

#Create a series of the tracked vessels from Excel-sheet
def vessel_list():
    input_DF = (pd.read_excel("trackedvessels.xlsx", index_col=None, header=0, dtype=str))
    return input_DF


#Write dataframe to excel
df, error_list = total_DF_creator(vessel_list(), dateRange="P90D")

print("---------------------------------")
print("Errors:")
for i in error_list:
    print(i)
uri = "mongodb+srv://{}:{}@mft.iz9okbe.mongodb.net/?retryWrites=true&w=majority".format(auth_dict["mongo"]["username"], auth_dict["mongo"]["password"])
client = MongoClient(uri)
db = client["VesselTimeline"]
collection = db["VesselCalls"]

data_dictionary = df.to_dict("records")
collection.delete_many({})
collection.insert_many(data_dictionary)

print("Succesfully uploaded to Atlas.")
client.close()