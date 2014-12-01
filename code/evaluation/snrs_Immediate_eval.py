# -*- coding: utf-8 -*-

__author__ = 'Anirudha'

# This script will predict the locations based on
# 1. User Preference
# 2. Business Acceptance
# 3. Immediate Friends Influence

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

#input variables.
inputUser = "c7Feo4TxZqwCvQ0rtk3igQ".lower()
inputCategory = "restaurants"
inputTopK = 25
immediateFriendThreshold = 2

#declare variables
userBusiness_dict = dict()
b2u_dict = dict()
baccept_dict = dict()
userRating_dict = dict()
immediateFriendsDict = dict()
immediateFriendsSet = set()
immediateFriendList = list()
inputUserBList = list()
toRecommend = list()
bidList = list()
businessList = list()
data = list()
data_dict = dict()
cityUser = set()
cityPrediction = set()

# Function that returns set of unique cities for the given business_id's. Input is list of business_id's.
def filterCity(ipList):
    city = set()
    for b in ipList:
        bid = b.lower()
        if bid.__contains__("-"):
            q = getQuery("business_id", bid)
        else:
            q = TermQuery("business_id", bid)
        resultB = conn.search(query=q, size=10000, indices=index_name, type=doc_typeB)
        city.add(resultB[0].city.lower())
    return city

# Function that filters the locations to recommend based on category and city.
# ipList: input of business_id's
# category: input category based on which we filter the locations
# cityInput: input set of city names based on which we filter the locations. This is generally the list of cities he inputUser has visited.
# cityOutput: output variable just used for future reference purpose.
# return: final filtered list of business_id's
def filterCategoryPerCity(ipList, category, cityInput, cityOutput):
    finalList = list()
    for b in ipList:
        try:
            bid = b.lower()
            if bid.__contains__("-"):
                q = getQuery("business_id", bid)
            else:
                q = TermQuery("business_id", bid)
            resultB = conn.search(query=q, size=10000, indices=index_name, type=doc_typeB)
            catList = map(lambda x: x.lower(),resultB[0].categories)
            city = resultB[0].city.lower()
            if category.lower() in catList:
                if city in cityInput:
                    finalList.append(bid)
                    cityOutput.add(city)
        except Exception,e:
            print "error3:",
            print e,
            print ":",
            print b
    return finalList

# If a search term has "-" hyphen sign in it (for eg: bhy6Tgr-uxc), then pyes is not able to search it. So this is a workaround idea to split the term in multiple sub terms like
# "bhy6Tgr" and "uxc" and used a boolean query to make sure both the sub terms are present in the search output. Is is an overhead function. In future might be pyes can come up with
# search possible with "-".
def getQuery(term, query):
    q = query.split("-")
    count = 0
    for t in q:
        if(t!="" and t!="_"):
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


#find the friends of the input user
if inputUser.__contains__("-"):
    q = getQuery("user_id", inputUser)
else:
    q = TermQuery("user_id", inputUser)
result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeU)
friendList = result[0].friends

#Lower the user_id of friends
friendList = map(lambda x: x.lower(), friendList)

#Find the business list of the input user
#Also create the userAcceptance list for probability calculation
userAcceptance = [0] * 6

if inputUser.__contains__("-"):
    q = getQuery("user_id", inputUser)
else:
    q = TermQuery("user_id", inputUser)

result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
size = len(result)
i = 0
businessRatingTemp_dict = dict()
while i < size:
    businessRatingTemp_dict[result[i].business_id.lower()] = result[i].stars
    inputUserBList.append(result[i].business_id.lower())
    userAcceptance[0] += 1
    userAcceptance[result[i].stars] += 1
    i += 1
userRating_dict[inputUser] = businessRatingTemp_dict
print "userAcceptance:",
print userAcceptance
userAcceptance = map(lambda x: x + 1, userAcceptance)
userAcceptance[0] += 4
print "userAcceptance:",
print userAcceptance
print "Input user business list length:",
print len(inputUserBList)


#Find the immediate friends of input user who have reviewed atleast k restaurants together
# 1. Find all the businesses of the immediate friends and store it in a businessList.
# 2. Create a user business dictionary, where key = user_id and value = list(businesses reviewed by that user.)
# 3. Create a user rating dictionary, where key - user and value = dictionary==>(key = business_id and value = rating))
# fList: inputUser friendList
# ipUserBList: inputUser business list
# k: number of restaurants to be co rated together for considering as immediate friends
def findImmediateFriends(fList,ipUserBList,userRating_dict,userBusiness_dict,k):
    print "Finding businesses to predict"
    toRecommendLocal = list()
    bidList = list()
    cnt = 0

    f = open('U2B_Dictionary.txt','r')
    print "Fetching U2B_Dictionary.txt";
    a = pickle.load(f)
    f.close()

    f = open('UserRating_Dictionary.txt','r')
    print "Fetching URating_Dictionary.txt";
    b = pickle.load(f)
    f.close()

    for r in fList:
        flag = "false"
        try:
            bidList = a[r]
        except:
            # In case if the value are not present in the stored dictionary, then fetch them at run time
            flag = "true"
            if r.__contains__("-"):
                q = getQuery("user_id", r)
            else:
                q = TermQuery("user_id", r)

            result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
            size = len(result)
            i = 0
            businessRatingLocal_dict = dict()
            while i < size:
                if result[i].business_id.lower() in businessRatingLocal_dict:
                    star = (businessRatingLocal_dict[result[i].business_id.lower()] + result[i].stars)/2
                    businessRatingLocal_dict[result[i].business_id.lower()] = star
                else:
                    businessRatingLocal_dict[result[i].business_id.lower()] = result[i].stars
                bidList.append(result[i].business_id.lower())
                i += 1

        intersect = list(set(ipUserBList) & set(bidList))
        minus = list(set(bidList) - set(ipUserBList))
        if ((len(intersect) > k) & len(minus) > 0):
            userBusiness_dict[r] = list(bidList)
            if flag == "true":
                userRating_dict[r] = dict(businessRatingLocal_dict)
            else:
                userRating_dict[r] = b[r]
            # toRecommendLocal = list(set(toRecommendLocal) | set(minus))
            # toRecommendLocal = list(set(toRecommendLocal) | set(bidList))
            immediateFriendList.append(r)
        # toRecommendLocal = list(set(toRecommendLocal) | set(minus))
        toRecommendLocal = list(set(toRecommendLocal) | set(bidList))
        del bidList[:]
        #del businessRatingLocal_dict
    # Delete both the dictionaries after their use is over. (They are huge in size. If not deleted, might cause memory management issue)
    del a
    del b
    return (toRecommendLocal)

toRecommend = findImmediateFriends(friendList,inputUserBList,userRating_dict,userBusiness_dict,immediateFriendThreshold)
print "To recommend list:",
print len(toRecommend)
print "Friend list:",
print len(friendList)
print "Immediate friend list:",
print len(immediateFriendList)

# Find the cities which input user has visited
cityUser = filterCity(inputUserBList)

# Filter the locations to recommend based on category and input user city details
toRecommend = filterCategoryPerCity(toRecommend,inputCategory,cityUser,cityPrediction)
print "To recommend list:",
print len(toRecommend)

toRecommend.sort()

print "************************************************"

# 1. Fetch the business acceptance ratings. Store that in a dictionary in which map = business_id and value = array[6]
# 2. Also fetch business to user dictionary where map = business_id and value = list of users who have reviewed this business.

f = open('B2U_Dictionary.txt','r')
print "Fetching B2U_Dictionary.txt"
a = pickle.load(f)
f.close()

f = open('BAccept_Dictionary.txt','r')
print "Fetching BAccept_Dictionary.txt"
b = pickle.load(f)
f.close()

# Create a subset of dictionary for ony the locations to recommend and delete the loaded dictionaries.
print "Creating the business user and business rating dictionaries"
j = 0
for r in toRecommend:
    b2u_dict[r] = a[r]
    baccept_dict[r] = b[r]
    j += 1

del a
del b

print "Started to calculate score..."
# For each location to predict calculate a numeric score
for r in toRecommend:
    r = r.lower()
    k = 1
    score = 0
    while k < 6:
        x = float(userAcceptance[k])/float(userAcceptance[0])
        y = float(baccept_dict[r][k])/float(baccept_dict[r][0])

        #If there is no immediate friends for the input use, then make z = 1. i.e calculate score based on user acceptance and business acceptance only.
        if len(immediateFriendList) != 0:
            z = 0.0
            l1 = b2u_dict[r]
            for u in l1:
                if(u in immediateFriendList):
                    list1 = userBusiness_dict[u]
                    intersect = list(set(inputUserBList) & set(list1))
                    minus = list(set(list1) - set(inputUserBList))
                    a = [1 for xt in range(9)]
                    for b in intersect:
                        a[userRating_dict[inputUser][b.lower()]-userRating_dict[u.lower()][b.lower()]]+=1
                    z += float(a[k-userRating_dict[u.lower()][r]])/float(sum(a))
        else:
            z = 1.0
        score += k*x*y*z
        k+=1
    data.append((r,score))
print "Completed score calculation...."

# Sort the predictions in descending order
data.sort(key=lambda r: r[1],reverse=True)
print "Length of predicted list:",
print len(data)
print data
print "Printing top",inputTopK,"items to predict:"
i = 0
for key in data:
    if i == inputTopK:
        break
    print key
    i+=1

# Assign rank to predicted list
i = 0
rank = 1
while i < len(data):
    data_dict[data[i][0]] = rank
    rank += 1
    i +=1

inputUserBList = filterCategoryPerCity(inputUserBList,inputCategory,cityUser,cityUser)
inputUserBList = list(set(inputUserBList))

print "Print the ranking of input user's visited locations predicted by algorithm"
i = 0
top15 = 0
top25 = 0
while i < len(inputUserBList):
    try:
        print inputUserBList[i],
        print ":",
        print data_dict[inputUserBList[i]]
        if data_dict[inputUserBList[i]] < 25:
            top25+=1
            if data_dict[inputUserBList[i]] < 15:
                top15+=1
    except:
        print ": Not present in predicted list"
    i+=1

print "Locations predicted from following cities:"
print cityUser
print cityPrediction

print "Evaluation Metric:"
print "top15:",
print float(top15)/float(len(inputUserBList))
print "top25:",
print float(top25)/float(len(inputUserBList))

print "Total Time:"
print datetime.now() - startTime
print "************************************************"


