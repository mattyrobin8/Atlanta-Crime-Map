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
            data = data.rename(columns = {'Report Date':keep_cols[0], 'UCR Literal':keep_cols[1], 'Neighborhood':keep_cols[2], 'Latitude':keep_cols[3], 'Longitude':keep_cols[4]})
        data = data[keep_cols]
        df = df.append(data, ignore_index = True)
    return df

def get_geodata(df, geolocator, lat_field, lon_field):
    '''Use Latitude and Longitude to find the Zip Codes'''
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    return location.raw

def create_match_file(df):
    match_df = pd.DataFrame(columns = match_cols)
    for index, value in df.items():
        temp_df = pd.DataFrame([[value['lat'], value['lon'], value['address']['house_number'], value['address']['road']]], columns = match_cols)
        match_df = match_df.append(temp_df, ignore_index = True)
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
keep_cols = ['rpt_date', 'UC2_Literal', 'neighborhood', 'lat', 'long']

#Columns to keep in matching dataframe
match_cols = ['lat', 'long', 'house_number', 'road']

#Establish Connection to geopy mechanism
geolocator = geopy.Nominatim(user_agent='bob')


#####################
####Run functions####
#####################

#Import Data
crime_df = import_data(file_list)

#Retrieve GeoData
geodata = crime_df[:10].apply(get_geodata, axis=1, geolocator=geolocator, lat_field='lat', lon_field='long')

#Extract zip codes and matching information from geodata
match_df = create_match_file(geodata)
print(match_df)

match_geo_df = pd.merge(crime_df, match_df, how="inner", on=['lat', 'long'], sort=True, suffixes=("_orig", "_match"), copy=True, validate=None)

#def main():
    #Import Data
    #crime_df = import_data(file_list)

#if __name__ == '__main__':
#    main()
