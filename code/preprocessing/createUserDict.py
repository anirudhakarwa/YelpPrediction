__author__ = 'Anirudha'

#The purpose of this file is to create the following dictionaries and store them on a file for future use.
# 1. User to Business Dictionary
# 2. User to Business Rating Dictionary

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

userBusiness_dict = dict()
userRating_dict = dict()
errorList = list()

#Open the user list file for all the user_id
f = open('userList.txt', 'r')
startTime = datetime.now()
totalUserList = pickle.load(f)
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

print "Total User List:",
print len(totalUserList)
j = 0
while j < len(totalUserList):
    try:
        businessRatingLocal_dict = dict()
        bidList = list()
        r = totalUserList[j][0].lower()
        print j,
        print r
        if r.__contains__("-"):
            q = getQuery("user_id", r)
        else:
            q = TermQuery("user_id", r)

        result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
        size = len(result)
        i = 0

        while i < size:
            #Check if the user has already rated that business before. If yes then take the average of two ratings.
            if result[i].business_id.lower() in businessRatingLocal_dict:
               star = (businessRatingLocal_dict[result[i].business_id.lower()] + result[i].stars)/2
               businessRatingLocal_dict[result[i].business_id.lower()] = star
            else:
                businessRatingLocal_dict[result[i].business_id.lower()] = result[i].stars
            bidList.append(result[i].business_id.lower())
            i += 1
        userBusiness_dict[r] = list(bidList)
        userRating_dict[r] = dict(businessRatingLocal_dict)
        del bidList[:]
        del businessRatingLocal_dict
    except:
        errorList.append(j)
    j += 1

print "Length of U2B:",
print len(userBusiness_dict)
print "Length of UserRating:",
print len(userRating_dict)
print "Length of ErrorList:",
print len(errorList)

#Dump the list to file
f = open('U2B_Dictionary.txt','w')
pickle.dump(userBusiness_dict,f)
f.close()

#Dump the list to file
f = open('UserRating_Dictionary.txt','w')
pickle.dump(userRating_dict,f)
f.close()

#Dump the error list
f = open('ErrorList_User.txt','w')
pickle.dump(errorList,f)
f.close()

print "Success."
print "Total Time:"
print datetime.now()-startTime

