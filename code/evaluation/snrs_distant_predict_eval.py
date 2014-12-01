# -*- coding: utf-8 -*-

__author__ = 'Anirudha'

# This script will predict the locations based on
# 1. User Preference
# 2. Business Acceptance
# 3. Immediate Friends Influence
# 4. Immediate Friend's Immediate Friends Influence

import pickle
import re
import math
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
inputUser = "w9oZOfJsvVQTw4bYZzvVCw".lower()
inputCategory = "restaurants"
inputTopK = 25
immediateFriendThreshold = 2

#declare variables
userBusiness_dict = dict()
b2u_dict = dict()
baccept_dict = dict()
uaccept_dict = dict()
userRating_dict = dict()
immediateFriendsDict = dict()
immediateFriendsSet = set()
immediateFriendList = list()
friendsDict = dict()
inputUserBList = set()
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

# Function that takes user_id as input and generates userAcceptance, userRating and user business list
def getUserAcceptanceAndRating(user,bList):
    userAcceptance = [0] * 6

    if user.__contains__("-"):
        q = getQuery("user_id", user)
    else:
        q = TermQuery("user_id", user)

    result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
    size = len(result)
    i = 0
    businessRatingTemp_dict = dict()
    while i < size:
        if result[i].business_id.lower() in businessRatingTemp_dict:
            star = (businessRatingTemp_dict[result[i].business_id.lower()] + result[i].stars)/2
            businessRatingTemp_dict[result[i].business_id.lower()] = star
        else:
            businessRatingTemp_dict[result[i].business_id.lower()] = result[i].stars
        bList.add(result[i].business_id.lower())
        userAcceptance[0] += 1
        userAcceptance[result[i].stars] += 1
        i += 1
    userRating_dict[user] = dict(businessRatingTemp_dict)
    userAcceptance = map(lambda x: x + 1, userAcceptance)
    userAcceptance[0] += 4
    uaccept_dict[user] = list(userAcceptance)
    del userAcceptance[:]
    del businessRatingTemp_dict

#Function that takes user_id as input and generates userAcceptance
def getUserAcceptance(user):
    userAcceptance = [0] * 6

    if user.__contains__("-"):
        q = getQuery("user_id", user)
    else:
        q = TermQuery("user_id", user)

    result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
    size = len(result)
    i = 0
    while i < size:
        userAcceptance[0] += 1
        userAcceptance[result[i].stars] += 1
        i += 1
    userAcceptance = map(lambda x: x + 1, userAcceptance)
    userAcceptance[0] += 4
    uaccept_dict[user] = list(userAcceptance)
    del userAcceptance[:]

# Function that takes user_id as input and generates userRating and user business list
def getUserRating(user,bList):

    if user.__contains__("-"):
        q = getQuery("user_id", user)
    else:
        q = TermQuery("user_id", user)

    result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeR)
    size = len(result)
    i = 0
    businessRatingTemp_dict = dict()
    while i < size:
        if result[i].business_id.lower() in businessRatingTemp_dict:
            star = (businessRatingTemp_dict[result[i].business_id.lower()] + result[i].stars)/2
            businessRatingTemp_dict[result[i].business_id.lower()] = star
        else:
            businessRatingTemp_dict[result[i].business_id.lower()] = result[i].stars
        bList.append(result[i].business_id.lower())
        i += 1
    userRating_dict[user] = dict(businessRatingTemp_dict)
    del businessRatingTemp_dict


#find the friends of the input user
if inputUser.__contains__("-"):
    q = getQuery("user_id", inputUser)
else:
    q = TermQuery("user_id", inputUser)
result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeU)
friendList = result[0].friends
friendsDict[inputUser] = friendList

#Lower the user_id of friends
friendList = map(lambda x: x.lower(), friendList)

#Get the userAcceptance, userRating and busienss list for input user
getUserAcceptanceAndRating(inputUser,inputUserBList)
print uaccept_dict[inputUser]
print userRating_dict[inputUser]
print len(userRating_dict[inputUser])
print inputUserBList
print len(inputUserBList)

#Find the immediate friends of input user who have reviewed atleast k restaurants together
# 1. Find all the businesses of the immediate friends and store it in a businessList.
# 2. Create a user business dictionary, where key = user_id and value = list(businesses reviewed by that user.)
# 3. Create a user rating dictionary, where key - user and value = dictionary==>(key = business_id and value = rating))
# fList: inputUser friendList
# ipUserBList: inputUser business list
# k: number of restaurants to be co rated together for considering as immediate friends
def findImmediateFriends(fList,ipUserBList,userRating_dict,userBusiness_dict,a,b,k):
    print "Finding businesses to predict"
    toRecommendLocal = list()
    bidList = list()
    cnt = 0

    for r in fList:
        flag = "false"
        try:
            bidList = a[r]
        except:
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
            #return toRecommendLocal
        # toRecommendLocal = list(set(toRecommendLocal) | set(minus))
        toRecommendLocal = list(set(toRecommendLocal) | set(bidList))
        del bidList[:]
        #del businessRatingLocal_dict
    return (toRecommendLocal)

# 1. Fetch the business acceptance ratings. Store that in a dictionary in which map = business_id and value = array[6]
# 2. Also fetch business to user dictionary where map = business_id and value = list of users who have reviewed this business.
f = open('U2B_Dictionary.txt','r')
print "Fetching U2B_Dictionary.txt";
a = pickle.load(f)
f.close()

f = open('UserRating_Dictionary.txt','r')
print "Fetching URating_Dictionary.txt";
b = pickle.load(f)
f.close()

toRecommend = findImmediateFriends(friendList,inputUserBList,userRating_dict,userBusiness_dict,a,b,immediateFriendThreshold)
immediateFriendsDict[inputUser] = immediateFriendList
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

# For each immediate friends of inputUser, try to find their immediate friends and store their userAcceptance and userRatings.
for r in immediateFriendList:
    try:
        print "Finding distant friends for:",
        print r
        del friendList[:]
        immediateFriendListTemp = list()
        if r.__contains__("-"):
            q = getQuery("user_id", r)
        else:
            q = TermQuery("user_id", r)
        result = conn.search(query=q, size=10000, indices=index_name, type=doc_typeU)
        friendList = result[0].friends
        friendList = map(lambda x: x.lower(), friendList)

        getUserAcceptance(r)
        ipUserBList = userBusiness_dict[r]


        for f in friendList:
            try:
                flag = "false"
                bidList = a[f]
            except:
                flag = "true"

            if flag == "true":
                getUserRating(f,bidList)

            intersect = list(set(ipUserBList) & set(bidList))
            minus = list(set(bidList) - set(ipUserBList))
            if ((len(intersect) > immediateFriendThreshold) & len(minus) > 0):
                userBusiness_dict[f] = list(bidList)
                if flag != "true":
                    userRating_dict[f] = b[f]
                immediateFriendListTemp.append(f)
            del bidList[:]
        immediateFriendsDict[r] = list(immediateFriendListTemp)
        del immediateFriendListTemp[:]
            #del businessRatingLocal_dict
        # return (toRecommendLocal)
    except Exception,e:
        print e

toRecommend.sort()

print "************************************************"
# 1. Create the business acceptance ratings. Store that in a dictionary in which map = business_id and value = array[6]
# 2. Also create business to user dictionary where map = business_id and value = list of users who have reviewed this business.

f = open('B2U_Dictionary.txt','r')
print "Fetching B2U_Dictionary.txt"
a = pickle.load(f)
f.close()

f = open('BAccept_Dictionary.txt','r')
print "Fetching BAccept_Dictionary.txt"
b = pickle.load(f)
f.close()

print "Creating the business user and business rating dictionaries"
j = 0
for r in toRecommend:
    b2u_dict[r] = a[r]
    baccept_dict[r] = b[r]
    j += 1

del a
del b

#Function that takes the business_id and user_id as input and calculates the score for that user_id with respect to the business_id
#This will be used when we are trying to incorporte immediate friend's immediate friends graph
def getScore(bid,uid):
    immediateFList = list()
    b2uList = list()
    ubList = list()
    immediateFList = immediateFriendsDict[uid]
    b2uList = b2u_dict[bid]
    ubList = userBusiness_dict[uid]
    flag = "false"
    score = 0

    # Check if any of the immediate user of uid has rated bid. If no, then no need to run for k = 1 to 6, just return 0.
    for i in immediateFList:
        if i in b2uList:
            flag = "true"
            break

    if flag == "true":
        k = 1
        while k < 6:
            x = float(uaccept_dict[uid][k])/float(uaccept_dict[uid][0])
            y = float(baccept_dict[bid][k])/float(baccept_dict[bid][0])
            z = 0.0
            for i in immediateFList:
                if i in b2uList:
                    ifbList = userBusiness_dict[i]
                    intersect = list(set(ubList) & set(ifbList))
                    # minus = list(set(ifbList) - set(ubList))
                    a = [1 for xt in range(9)]
                    for b in intersect:
                        a[userRating_dict[uid][b.lower()]-userRating_dict[i.lower()][b.lower()]]+=1
                    z += float(a[k-userRating_dict[i.lower()][bid]])/float(sum(a))
            score += k*x*y*z
            k+=1

    del immediateFList
    del b2uList
    del ubList
    return score

print "Started to calculate score..."
# For each location to predict calculate a numeric score
for r in toRecommend:
    distantRating_dict = dict()
    try:
        # print "For r:",
        # print r.lower()
        r = r.lower()
        b2userList = b2u_dict[r]
        #For each immediate friends try to predict the score
        for i in immediateFriendList:
            # print "For i:",
            # print i.lower()
            try:
                if i in b2userList:
                    # print "userRating:",
                    # print userRating_dict[i]
                    distantRating_dict[i] = userRating_dict[i][r]
                else:
                    # print "Calling z:",
                    z = getScore(r,i)
                    # print "Got z as:",
                    # print z
                    if z != 0:
                        distantRating_dict[i] = int(math.ceil(z))
                        userRating_dict[i][r] = int(math.ceil(z))
            except Exception, e:
                print "error1:",
                print e

        if len(distantRating_dict) != 0:
            k = 1
            score = 0
            while k < 6:
                for key in distantRating_dict:
                    # print "For key:",
                    # print key.lower()
                    x = float(uaccept_dict[inputUser][k])/float(uaccept_dict[inputUser][0])
                    y = float(baccept_dict[r][k])/float(baccept_dict[r][0])
                    z = 0.0
                    ifbList = userBusiness_dict[key]
                    intersect = list(set(inputUserBList) & set(ifbList))
                    # minus = list(set(ifbList) - set(ubList))
                    a = [1 for xt in range(9)]
                    for b in intersect:
                        a[userRating_dict[inputUser][b.lower()]-userRating_dict[key][b.lower()]]+=1
                    z += float(a[k-userRating_dict[key][r]])/float(sum(a))
                score += k*x*y*z
                k+=1
            # print score
            data.append((r,score))
    except Exception, e:
        print "error2:",
        print e
    del distantRating_dict

print "Completed score calculation...."
data.sort(key=lambda r: r[1],reverse=True)
print "Length of predicted list:",
print len(data)

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
top15 = 0
top25 = 0
for i in inputUserBList:
    try:
        print i,
        print ":",
        print data_dict[i]
        if data_dict[i] < 25:
           top25+=1
           if data_dict[i] < 15:
               top15+=1
    except:
        print i,
        print ": Not present in predicted list"

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


