# coding=utf-8

from pyspark import SparkConf, SparkContext
import collections
"""
Find out how many X star ratings / rating types there are, grouped by rating values (1, 2, 3, 4, 5 stars)

"""

"""
u.data
contains all the movie ratings
sample (top 3 lines)
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116

u.item
contains the lookup table for all the movie IDs to movie names
sample (top 3 lines)
1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
3|Four Rooms (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Four%20Rooms%20(1995)|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
"""
# setAppName is for us to identify the job in Spark Web UI by its name
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
"""
lines is similar to this (sample top 3).
196     242     3       881250949
186     302     3       891717742
22      377     1       878887116

And from the book, it explains each column's meaning, i.e.:
userID  movie ID    rating value    timestamp

Every line corresponds to one value in RDD.
"""

"""
ratings = lines.map(lambda x: x.split()[2])
The index=2 column is the rating value (1, 2, 3, 4, 5)

ratings is similar to:
3
3
1
"""
ratings = lines.map(lambda x: x.split()[2])
# for rating in ratings.collect():
#     print(rating)

result = ratings.countByValue()
result_sorted = sorted(result.items())

"""
ratings.countByValue()
defaultdict(<class 'int'>, {'4': 34174, '1': 6110, '5': 21201, '3': 27145, '2': 11370})

result.items() is similar to:
dict_items([('4', 34174), ('1', 6110), ('5', 21201), ('3', 27145), ('2', 11370)])

sorted(result.items()) is similar to:
[('1', 6110), ('2', 11370), ('3', 27145), ('4', 34174), ('5', 21201)]
"""

# Straight-up Python code (not Spark)
# Sort based on the key (key is rating value)
sortedResults = collections.OrderedDict(result_sorted)
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

"""
Result:
1 6110
2 11370
3 27145
4 34174
5 21201
"""
# THE END
