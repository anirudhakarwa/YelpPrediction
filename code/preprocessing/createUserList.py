__author__ = 'Anirudha'

#The purpose of this file is to create a list of all users and store them in a file for future use

import pickle
from pyes import *
from pyes.es import ResultSet
from datetime import datetime

startTime = datetime.now()
print "Welcome"

index_name = "yelp"
doc_type = "user"
conn = ES('127.0.0.1:9200')
conn.default_indices=[index_name]
userList = list()

#Fetch all the results from yelp/user index 
q = MatchAllQuery() 
results = conn.search(query = q,indices=[index_name], type=doc_type, fields="user_id")

i = 0
#Append the results to userList.
print "Creating user list..."
for r in results:
	i+=1
	print i
	userList.append(r.user_id)

print len(userList)	
	
#Dump the list to file
f = open('userList.txt','w')
pickle.dump(userList,f)
f.close()
print "Success."
print "Total Time:"
print datetime.now()-startTime


