#####Define Functions#####

import pandas as pd

def import_data(file_location):
    '''Import the data and append to one dataframe'''
    df  = pd.DataFrame(columns=keep_cols)
    for datafile in file_location:
        data = pd.read_csv(datafile)
        if datafile == r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv":
            data = data.rename(columns = {'Report Date': keep_cols[0], 'UCR Literal': keep_cols[1], 'Neighborhood': keep_cols[2], 'Latitude': keep_cols[3], 'Longitude': keep_cols[4]}, inplace = True)
        data = data[keep_cols]
        df = df.append(data, ignore_index = True)
    return df		

#####Define objects#####

#list files
file1 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2021.csv"
file2 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020(NEW RMS 9-30 12-31).csv"
file3 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020-OldRMS-09292020.csv"
file4 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv"
file_list = [file1, file2, file3, file4]

#Which columns to keep
keep_cols = ['rpt_date','UC2_Literal','neighborhood','lat','long']


#####Run functions#####

def main():
    #Import Data
    crime_df = import_data(file_list)
    print(crime_df)

if __name__ == '__main__':
    main()