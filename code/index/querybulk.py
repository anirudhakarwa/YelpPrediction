# __author__ = 'Anirudha'
# file directory = file to index
# _index = "yelp"
# _type = "review". This is the index directory for indexing the documents of review type. Similarly, 
# _type = "user". This is the index directory for indexing the documents of user type
# _type = "business". This is the index directory for indexing the documents of business type
# This is a bulk indexing process i.e. it reads a batch of lines from file and indexes them at a go. It is much faster than sequential indexing.

import os
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

startTime = datetime.now()

with open('C:/Users/Anirudha/Documents/Masters/Datasets/Yelp/yelp_dataset_challenge_academic_dataset/review/segmentac.json') as fopen:
	content = fopen.readlines()

es = Elasticsearch()
j = 0
actions = []

while (j < 50000):
    action = {
        "_index": "yelp",
        "_type": "review",
        #"_id": j,
        "_source": content[j]
        }
    actions.append(action)
    j += 1

    while (len(actions) > 10000 and len(actions) % 10000 == 0):
        helpers.bulk(es, actions)
        del actions[0:len(actions)]
        break

if (len(actions) > 0):
    helpers.bulk(es, actions)
	
print(datetime.now()-startTime)	
