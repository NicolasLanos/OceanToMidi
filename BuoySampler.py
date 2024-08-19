import requests
import xml.etree.ElementTree as ET
import csv

# Define the URL for the NDBC active stations XML feed
url = "https://www.ndbc.noaa.gov/activestations.xml"

# Ocean classification function based on latitude and longitude
def classify_ocean(latitude, longitude):
    # Convert latitude and longitude to float
    lat = float(latitude)
    lon = float(longitude)
    
    # Classify based on latitude and longitude ranges (simplified)
    if lat > 60:
        return "Arctic Ocean"
    elif lat < -60:
        return "Southern Ocean"
    elif -60 < lat < 30 and 20 < lon < 180:
        return "Indian Ocean"
    elif -60 < lat < 30 and -70 < lon < 20:
        return "South Atlantic Ocean"
    elif 30 <= lat < 60 and -70 < lon < 20:
        return "North Atlantic Ocean"
    elif 30 <= lat < 60 and 20 < lon < 180:
        return "North Pacific Ocean"
    elif -60 < lat < 30 and (lon > 180 or lon < -70):
        return "South Pacific Ocean"
    else:
        return "Unclassified Ocean"

# Make the GET request to retrieve the station data
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the XML content
    root = ET.fromstring(response.content)
    
    # Lists to store station IDs and their corresponding oceans
    station_ids = []
    station_oceans = []
    
    # Loop through each station entry in the XML feed and filter by type="BUOY"
    for station in root.findall('.//station'):
        station_type = station.get('type', '')
        if station_type == "buoy":
            station_id = station.get('id', '')
            latitude = station.get('lat', '')
            longitude = station.get('lon', '')
            
            # Classify the station's ocean based on latitude and longitude
            ocean = classify_ocean(latitude, longitude)
            
            # Add the station ID and its classified ocean to the lists
            station_ids.append(station_id)
            station_oceans.append(ocean)
    
    # Combine station IDs and oceans into a list of tuples
    stations_with_oceans = list(zip(station_ids, station_oceans))
    
    # Print the results
    print("Station IDs with corresponding oceans:", stations_with_oceans)
else:
    print(None)


####################################################################################################################################################################
####################################################################################################################################################################
####################################################################################################################################################################
# Define the wanted ocean
WantedOcean = 'South Atlantic Ocean'
####################################################################################################################################################################
####################################################################################################################################################################
####################################################################################################################################################################
# List of Station IDs with corresponding oceans (assuming this is already defined in the previous part)
# Example: stations_with_oceans = [('12345', 'South Atlantic Ocean'), ('67890', 'North Atlantic Ocean'), ...]

# Filter the station IDs that match the selected ocean
station_ids = [station_id for station_id, ocean in stations_with_oceans if ocean == WantedOcean]

# Print the filtered station IDs
print("Filtered Station IDs for", WantedOcean, ":", station_ids)


import requests
import pandas as pd
import numpy as np
import h5py
from datetime import datetime
import time


# NDBC Base URL
base_url = "https://www.ndbc.noaa.gov/data/realtime2/"

# Measurement parameters to be fetched
parameters = ['WDIR', 'WSPD', 'WVHT', 'PRES', 'WTMP']

# HDF5 file for storing the data
hdf5_file = "buoy_data.h5"

# Create HDF5 file and dataset
with h5py.File(hdf5_file, "a") as f:
    # Check if datasets already exist, otherwise create them
    if 'stations' not in f:
        f.create_dataset("stations", data=np.array(station_ids, dtype='S'))
        f.create_dataset("parameters", data=np.array(parameters, dtype='S'))
        f.create_dataset("measurements", (0, len(station_ids), len(parameters)), maxshape=(None, len(station_ids), len(parameters)), dtype='f')

# Function to fetch and store the data
def fetch_and_store_data():
    data_matrix = []
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    for station_id in station_ids:
        try:
            # Build the data URL for the station's real-time data
            data_url = f"{base_url}{station_id}.txt"
            
            # Request the data from NDBC
            response = requests.get(data_url)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Split the data into lines
                station_data = response.text.splitlines()
                
                # Get the header row to determine the column positions
                header = station_data[0].split()
                
                # Find the column indices for required parameters
                column_map = {var: header.index(var) for var in parameters if var in header}
                
                # Initialize measurement data for the current station
                measurements = np.full((len(parameters),), np.nan)  # Start with NaN values
                
                # Loop through the data rows
                for line in station_data[1:]:
                    columns = line.split()

                    # Ensure the line has sufficient data and extract the most recent measurement
                    if len(columns) >= max(column_map.values()) + 1:
                        try:
                            # Extract the data for the required parameters
                            for param, idx in column_map.items():
                                value = columns[idx]
                                # Only convert numeric values; skip non-numeric values like 'degT'
                                if value.replace('.', '', 1).isdigit():
                                    measurements[parameters.index(param)] = float(value)
                        except ValueError:
                            continue  # Skip if there's any issue with conversion
                        
                        # Print the data being processed for real-time display
                        print(f"Station {station_id} | Time: {columns[0]} | Data: {measurements}")
                        
                        break  # Break after first valid line (latest measurement)
                
                # Add the measurements to the data matrix
                data_matrix.append(measurements)
            else:
                print(f"Failed to retrieve data for station {station_id}: {response.status_code}")
                data_matrix.append([np.nan] * len(parameters))  # Fill with NaNs in case of failure

        except Exception as e:
            print(f"Error processing data for station {station_id}: {e}")
            data_matrix.append([np.nan] * len(parameters))  # Fill with NaNs in case of error

    # Convert the data matrix to a NumPy array
    data_matrix = np.array(data_matrix)
    
    # Append the new data to the HDF5 file
    with h5py.File(hdf5_file, "a") as f:
        # Retrieve the existing measurements dataset
        measurements_dataset = f["measurements"]
        
        # Determine the current size and the new size after appending
        current_size = measurements_dataset.shape[0]
        new_size = current_size + 1
        
        # Resize the dataset to accommodate the new data
        measurements_dataset.resize(new_size, axis=0)
        
        # Append the new data (as the last layer)
        measurements_dataset[current_size, :, :] = data_matrix

    print(f"Data stored at {current_time}")

# Schedule the fetching process to run every 10 minutes
def schedule_data_fetching():
    while True:
        # Fetch and store the data
        fetch_and_store_data()
        
        # Sleep for 10 minutes before the next fetch
        time.sleep(600)

# Start the data fetching process
schedule_data_fetching()
