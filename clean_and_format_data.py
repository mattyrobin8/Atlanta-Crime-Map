######################
###Import Libraries###
######################

import pandas as pd
import pandasql as ps
import time


######################
###Define Functions###
######################

def import_data(file_location):
	'''Read in Crime Data'''
	df = pd.read_csv(file_location, low_memory = False)
	df[['Crime','Crime Extra']] = df.UC2_Literal.str.split("-",expand=True)
	return df


######################
####Create objects####
######################

#List files
file_location = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"


#####################
####Run functions####
#####################

def main():

    #Import Data
	crime_df = import_data(file_location)
	print(crime_df)

	#Create ATL crime dataframe
	crime_query = """
			select  strftime('%Y', rpt_date) as year
					,Crime
					,count(*) as crime
			from crime_df
			group by Crime
					 ,strftime('%Y', rpt_date)
		"""
	atlcrime_df = ps.sqldf(crime_query)

	query1 = """
			select  min(rpt_date) as date
			from crime_df
		"""
	print(ps.sqldf(query1))

	#Create ATL population dataframe
	data = [['2018',459600],['2019',470500],['2020',478200],['2021',(478200-470500) + 478200]]
	atlpop_df  = pd.DataFrame(data, columns = ['year', 'population'])
	print(atlpop_df)


#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))