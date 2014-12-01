__author__ = 'Anirudha'

# It is just a simple script to find categories of all the restaurants, the input user have visited till now

import pickle
import re
from pyes import *
from pyes.es import ResultSet
from datetime import datetime

startTime = datetime.now()
print "Welcome"

index_name = "yelp"
doc_typeR = "review"
doc_typeB = "business"
doc_typeU = "user"
conn = ES('127.0.0.1:9200')
conn.default_indices = [index_name]

inputUser = "jnvitd54whawdwdyus5lna".lower()

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

categories = set()

if inputUser.__contains__("-"):
    q = getQuery("user_id", inputUser)
else:
    q = TermQuery("user_id", inputUser)
result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
i = 0
while i < len(result):
    if result[i].business_id.lower().__contains__("-"):
        que = getQuery("business_id", result[i].business_id.lower())
    else:
        q = TermQuery("business_id", result[i].business_id.lower())
    resultB = conn.search(query=q, size=10000, indices=index_name, type=doc_typeB)
    print resultB[0].business_id
    print resultB[0].categories
    #Lower the user_id of friends
    catList = map(lambda x: x.lower(),resultB[0].categories )
    for c in catList:
        categories.add(c)
    i+=1
print categories
