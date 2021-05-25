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
	df['Crime'] = df['Crime'].replace(['MANSLAUGHTER'],'HOMICIDE')
	return df


######################
####Create objects####
######################

#List files
file_location = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"

#Create ATL population dataframe
data = [['2018',459600],['2019',470500],['2020',478200],['2021',(478200-470500) + 478200]]
atlpop_df = pd.DataFrame(data, columns = ['year', 'population'])


######################
####Create queries####
######################

#Create ATL crime dataframe
crime_query = 	"""
				select  	strftime('%Y', rpt_date) as year
							,strftime('%m', rpt_date) as month
							,strftime('%Y%m', rpt_date) as year_month
							,zipcode
							,Crime
							,count(*) as total_crime
				from 		crime_df
				group by 	Crime
							,zipcode
							,strftime('%Y', rpt_date)
							,strftime('%m', rpt_date)
							,strftime('%Y%m', rpt_date)
				order by	strftime('%Y%m', rpt_date)
							,zipcode
							,crime
				"""

#Join ATL crime and population dataframes
crimepop_query = """
				select  	crime.year
							,month
							,year_month
							,zipcode
							,Crime
							,total_crime
							,population
				from 		atlcrime_df crime
				join		atlpop_df pop
				on			crime.year = pop.year
				where		zipcode not in ('None')
				order by 	year_month
				"""

#Pull numerator for extrapolation of 2021 data
numerator_2021_query = """
				select  	year
							,Crime
							,sum(total_crime) as total_crime
				from 		atlcrime_df
				where		zipcode not in ('None')
				and			month in ('01','02','03','04')
				and			year <= 2020
				group by	year
							,Crime
				order by 	year
							,Crime
				"""

#Pull denominator for extrapolation of 2021 data
denominator_2021_query = """
				select  	year
							,Crime
							,sum(total_crime) as total_crime
				from 		atlcrime_df
				where		zipcode not in ('None')
				and			year <= 2020
				group by	year
							,Crime
				order by 	year
				"""


#####################
####Run functions####
#####################

def main():

    #Import processed crime dataframe
	crime_df = import_data(file_location)

	#Run ATL crime query
	atlcrime_df = ps.sqldf(crime_query)

	#Run ATL population query
	atlcrimepop_df = ps.sqldf(crimepop_query)
	#atlcrimepop_df['crimes_per_100K'] = (atlcrimepop_df['total_crime'] / atlcrimepop_df['population']) * 100000

	#Run 2021 numerator
	numerator_2021_df = ps.sqldf(numerator_2021_query)
	print(numerator_2021_df)

	#Run 2021 denominator
	denominator_2021_df = ps.sqldf(denominator_2021_query)
	print(denominator_2021_df)


#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))