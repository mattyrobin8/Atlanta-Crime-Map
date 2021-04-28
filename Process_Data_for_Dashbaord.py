######################
###Import Libraries###
######################

import pandas as pd
import geopy


######################
###Define Functions###
######################

def import_data(file_location):
    '''Import the data and append to one dataframe'''
    df  = pd.DataFrame(columns = keep_cols)
    for datafile in file_location:
        data = pd.read_csv(datafile, low_memory = False)
        if datafile == r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv":
            data = data.rename(columns = {'Report Date':keep_cols[0], 'Location':keep_cols[1], 'UCR Literal':keep_cols[2], 'Neighborhood':keep_cols[3], 'Latitude':keep_cols[4], 'Longitude':keep_cols[5]})
        data = data[keep_cols]
        df = df.append(data, ignore_index = True)
    return df

def get_geodata(df, geolocator, lat_field, lon_field):
    '''Use Latitude and Longitude to find the Zip Codes'''
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    return location.raw

def create_match_file(df):
    '''Create DataFrame from GeoData to match back to original data to find Zip Codes'''
    match_df = pd.DataFrame(columns = match_cols)
    for index, value in df.items():
        temp_df = pd.DataFrame([[value['lat'], value['lon'], value['address']['postcode']]], columns = match_cols)
        match_df = match_df.append(temp_df, ignore_index = True)
    match_df['lat'] = pd.to_numeric(match_df['lat'])
    match_df['long'] = pd.to_numeric(match_df['long'])
    return match_df


######################
####Create objects####
######################

#list files
file1 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2021.csv"
file2 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020(NEW RMS 9-30 12-31).csv"
file3 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020-OldRMS-09292020.csv"
file4 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv"
file_list = [file1, file2, file3, file4]

#Columns to keep in original dataframe
keep_cols = ['rpt_date', 'location', 'UC2_Literal', 'neighborhood', 'lat', 'long']

#Columns to keep in matching dataframe
match_cols = ['lat', 'long', 'zipcode']

#Establish Connection to geopy mechanism
geolocator = geopy.Nominatim(user_agent='bob')


#####################
####Run functions####
#####################

def main():

    #Import Data
    crime_df = import_data(file_list)

    #Retrieve GeoData
    geodata = crime_df[:100].apply(get_geodata, axis=1, geolocator=geolocator, lat_field='lat', lon_field='long')

    #Extract zip codes and matching information from geodata
    match_df = create_match_file(geodata)

    #Merge the dataframes to retrieve zipcode from match df
    match_geo_df = pd.merge(crime_df, match_df, how="inner", left_index=True, right_index=True, sort=True, suffixes=("_orig", "_match"), copy=True, validate=None)

    #Export matched df
    match_geo_df.to_csv(r'C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\crime_with_zips.csv', index = False)

if __name__ == '__main__':
    main()