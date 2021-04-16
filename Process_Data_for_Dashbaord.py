######################
###Import Libraries###
######################

import pandas as pd
import geopy

#####Define Functions#####

def import_data(file_location):
    '''Import the data and append to one dataframe'''
    df  = pd.DataFrame(columns=keep_cols)
    for datafile in file_location:
        data = pd.read_csv(datafile, low_memory=False)
        if datafile == r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv":
            data = data.rename(columns = {'Report Date':keep_cols[0], 'UCR Literal':keep_cols[1], 'Neighborhood':keep_cols[2], 'Latitude':keep_cols[3], 'Longitude':keep_cols[4]})
        data = data[keep_cols]
        df = df.append(data, ignore_index = True)
    return df

def get_zipcode(df, geolocator, lat_field, lon_field):
    '''Take Latitude and Longitude and find the Zip Codes'''
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    geo_data = location.raw
    print(geo_data['lat'])
    print(geo_data['lon'])
    print(geo_data['address'])
    #return location.raw


######################
####Define objects####
######################

#list files
file1 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2021.csv"
file2 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020(NEW RMS 9-30 12-31).csv"
file3 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020-OldRMS-09292020.csv"
file4 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv"
file_list = [file1, file2, file3, file4]

#Which columns to keep
keep_cols = ['rpt_date','UC2_Literal','neighborhood','lat','long']

#Establish Connection to geopy mechanism
geolocator = geopy.Nominatim(user_agent='bob')


#####################
####Run functions####
#####################

#Import Data
crime_df = import_data(file_list)

#Extract Zip Codes
zipcodes = crime_df.apply(get_zipcode, axis=1, geolocator=geolocator, lat_field='lat', lon_field='long')

#def main():
    #Import Data
    #crime_df = import_data(file_list)

#if __name__ == '__main__':
#    main()
