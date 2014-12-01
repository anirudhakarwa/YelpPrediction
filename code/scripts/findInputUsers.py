__author__ = 'Anirudha'

# This is a script which gives users that have
# 1. Visited at least 10 locations
# 2. At least 1 immediate (people who have at least co rated 3 locations together) friends

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

visitedLocations = 10
immediateFriendThreshold = 2

ipUserBList = list()
bidList = list()

f = open('U2B_Dictionary.txt','r')
print "Fetching U2B_Dictionary.txt";
a = pickle.load(f)
f.close()

print type(a)
print len(a)
for key in a:
    count = 0
    if len(a[key]) > visitedLocations:
        try:
            ipUserBList = a[key]
            # print "ipUserBList"
            if key.__contains__("-"):
                q = getQuery("user_id", key)
            else:
                q = TermQuery("user_id", key)
            result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeU)
            friendList = result[0].friends
            friendList = map(lambda x: x.lower(), friendList)

            for f in friendList:
                try:
                    bidList = a[f]
                    intersect = list(set(ipUserBList) & set(bidList))
                    # minus = list(set(bidList) - set(ipUserBList))
                    if (len(intersect) > immediateFriendThreshold):
                        count+=1
                except Exception, e:
                    continue
                    # print e
            if count > 0:
                print key,
                print ":",
                print count
        except Exception, e:
            continue