#!/usr/bin/bash
# __author__ = 'Anirudha'
# filename = file to index
# yelp/review is the index directory for indexing the documents of review type. Similarly, 
# yelp/user is the index directory for indexing the documents of user type
# yelp/business is the index directory for indexing the documents of business type
# This is a sequential indexing process i.e. it reads a single line from file and indexes it.

filename="review.json"
id=1
while read line
do
	curl -XPOST 'http://localhost:9200/yelp/review/' -d "$line"
	echo $id
	let id=id+1
	
done < "$filename"