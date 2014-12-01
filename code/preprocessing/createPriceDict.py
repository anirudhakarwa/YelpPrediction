__author__ = 'Anirudha'

#The purpose of this file is to create the following dictionary and store them on a file for future use.
# 1. Price Attribute Dictionary

import pickle
import re
from pyes import *
from pyes.es import ResultSet
from datetime import datetime

startTime = datetime.now()
print "Welcome"

index_name = "yelp"
doc_typeB = "business"
conn = ES('127.0.0.1:9200')
conn.default_indices = [index_name]
priceAttribute = set()
errorList = list()
businessPrice_Dict = dict()

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

    # Store the price range of the business if available else ignore.
    try:
        r = totalBusinessList[j][0].lower()
        print j
        if r.__contains__("-"):
            q = getQuery("business_id", r)
        else:
            q = TermQuery("business_id", r)

        result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeB)
        businessPrice_Dict[r] = result[0].attributes["Price Range"]
        priceAttribute.add(result[0].attributes["Price Range"])
    except:
        errorList.append(j)
    j += 1

print "Length of priceAttribute:",
print len(priceAttribute)
print "Length of businessPriceDitc:",
print len(businessPrice_Dict)
print businessPrice_Dict
print "Length of errorList:",
print len(errorList)
print "priceAttribute:",
print priceAttribute

# #Dump the list to file
f = open('B2P_Dictionary.txt','w')
pickle.dump(businessPrice_Dict,f)
f.close()

# #Dump the error list
f = open('ErrorList_Price.txt','w')
pickle.dump(errorList,f)
f.close()

print "Success."
print "Total Time:"
print datetime.now()-startTime