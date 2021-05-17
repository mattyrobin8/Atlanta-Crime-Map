######################
###Import Libraries###
######################

from os import error
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

    #Import
	crime_df = import_data(file_location)

	#Create ATL crime dataframe
	crime_query = """
			select  	strftime('%Y', rpt_date) as year
						,strftime('%Y%m', rpt_date) as year_month
						,Crime
						,count(*) as total_crime
			from 		crime_df
			group by 	Crime
						,strftime('%Y', rpt_date)
					 	,strftime('%Y%m', rpt_date)
			order by 	strftime('%Y%m', rpt_date)
		"""
	atlcrime_df = ps.sqldf(crime_query)
	#print(atlcrime_df)

	#Create ATL population dataframe
	data = [['2018',459600],['2019',470500],['2020',478200],['2021',(478200-470500) + 478200]]
	atlpop_df  = pd.DataFrame(data, columns = ['year', 'population'])
	#print(atlpop_df)

	#Join ATL crime and population dataframes
	crimepop_query = """
			select  	year_month
						,Crime
						,total_crime
						,population
						,(total_crime/population) as crime_per_pop
						,(total_crime/population) * 100000 as crimes_per_1K
			from 		atlcrime_df crime
			join		atlpop_df pop
			on			crime.year = pop.year
		"""
	atlcrimepop_df = ps.sqldf(crimepop_query)
	print(atlcrimepop_df)


#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))