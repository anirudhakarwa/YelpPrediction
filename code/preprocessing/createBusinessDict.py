__author__ = 'Anirudha'

#The purpose of this file is to create the following dictionaries and store them on a file for future use.
# 1. Business to User Dictionary
# 2. Business Acceptance Dictionary

import pickle
import re
from pyes import *
from pyes.es import ResultSet
from datetime import datetime

startTime = datetime.now()
print "Welcome"

index_name = "yelp"
doc_typeR = "review"
conn = ES('127.0.0.1:9200')
conn.default_indices = [index_name]
b2u_dict = dict()
baccept_dict = dict()
errorList = list()

#Open the business list file for all the business_id
f = open('businessList.txt', 'r')
startTime = datetime.now()
totalBusinessList = pickle.load(f)
print datetime.now()-startTime
startTime = datetime.now()
f.close()

def getQuery(term, query):
    q = query.split("-")
    count = 0
    for t in q:
        if(t!=""):
            if(count == 0):
                q1 = TermQuery(term,t)
                count+=1
            elif(count==1):
                 q2 = TermQuery(term,t)
                 count+=1
            else:
                q3 = TermQuery(term,t)
                count+=1
                break

    if count == 1:
        q = BoolQuery(must=[q1])
    elif count ==2:
        q = BoolQuery(must=[q1, q2])
    else:
        q = BoolQuery(must=[q1, q2,q3])
    return q

print "Total Business List:",
print len(totalBusinessList)
j = 0
while j < len(totalBusinessList):
    try:
        r = totalBusinessList[j][0].lower()
        print j

        if r.__contains__("-"):
            q = getQuery("business_id", r)
        else:
            q = TermQuery("business_id", r)

        result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
        size = len(result)
        i = 0
        b2u_dict[r] = list()
        businessRatingTemp_list = [0 for x in range(6)]
        while i < size:
            b2u_dict[r].append(result[i].user_id.lower())
            businessRatingTemp_list[0] += 1
            businessRatingTemp_list[result[i].stars] += 1
            i += 1
        businessRatingTemp_list = map(lambda x: x + 1, businessRatingTemp_list)
        businessRatingTemp_list[0] += 4
        baccept_dict[r] = businessRatingTemp_list
    except:
        errorList.append(j)
    j += 1

print "Length of B2U:",
print len(b2u_dict)
print "Length of BAccept:",
print len(baccept_dict)

#Dump the list to file
f = open('B2U_Dictionary.txt','w')
pickle.dump(b2u_dict,f)
f.close()

#Dump the list to file
f = open('BAccept_Dictionary.txt','w')
pickle.dump(baccept_dict,f)
f.close()

#Dump the error list
f = open('ErrorList.txt','w')
pickle.dump(errorList,f)
f.close()

print "Success."
print "Total Time:"
print datetime.now()-startTime