__author__ = 'Anirudha'

#The purpose of this file is to create a list of all businesses and store them in a file for future use
import re
import pickle
from pyes import *
from pyes.es import ResultSet
from datetime import datetime

startTime = datetime.now()
print "Welcome"

index_name = "yelp"
doc_typeB = "business"
conn = ES('127.0.0.1:9200')
conn.default_indices=[index_name]
businessList = list()

#Fetch all the results from yelp/business index 
q = MatchAllQuery() 
results = conn.search(query = q,indices=[index_name], type=doc_typeB, fields="business_id")

#Append the results to businessList.
print "Creating business list..."
for r in results:
	businessList.append(r.business_id)

#Dump the list to file
f = open('businessList.txt','w')
pickle.dump(businessList,f)
f.close()
print "Success."
print "Total Time:"
print datetime.now()-startTime





