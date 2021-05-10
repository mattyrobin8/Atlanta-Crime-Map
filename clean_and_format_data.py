######################
###Import Libraries###
######################

import pandas as pd
import pandasql as ps
import time


######################
###Define Functions###
######################



######################
####Create objects####
######################


datafile = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"
crime_df = pd.read_csv(datafile, low_memory = False)
#crime_df[['Crime','Crime Extra']] = df.UC2_Literal.str.split("-",expand=True)
print(crime_df)

query = """
			select  strftime('%Y', rpt_date) as year
					,Crime
					,count(*) as crime
			from crime_df
			group by Crime
					 ,strftime('%Y', rpt_date)
		"""

query1 = """
			select  min(rpt_date) as date
			from crime_df
		"""
print(ps.sqldf(query1))

#####################
####Run functions####
#####################

#def main():


#Run Main script and record runtime
#if __name__ == '__main__':
#    start_time = time.time()
#    main()
#    print("--- %s seconds ---" % (time.time() - start_time))