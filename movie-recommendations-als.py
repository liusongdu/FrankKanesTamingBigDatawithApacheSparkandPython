# coding=utf-8

import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating
import pyspark
print("pyspark version: ", pyspark.__version__)
# 2.1.1+hadoop2.7
print("Python version: ", sys.version)
# Python version:  3.5.2 ...


def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM", encoding='utf-8') as f:
        for line in f:
            line = line.encode('utf-8').decode('utf-8-sig')
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]  # .decode('ascii', 'ignore')
    return movieNames


conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommensdationsALS")
sc = SparkContext(conf=conf)
sc.setCheckpointDir('checkpoint')

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")

ratings = data.map(lambda l: l.split("\t")).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()
# for rating in ratings.collect():
#     print(rating)

# Build the recommendation model using Alternating Least Squares
print("\nTraining recommendation model...")
rank = 10
# Lowered numIterations to ensure it works on lower-end systems
numIterations = 6
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratings.filter(lambda l: l[0] == userID)
for rating in userRatings.collect():
    print(nameDict[int(rating[1])] + ": " + str(rating[2]))

print("\nTop 10 recommendations:")

recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] + " score " + str(recommendation[2]))

"""
Execution result:

# 此处不应该是0，应该是已有的，比如50
(User) C:\SparkCourse>spark-submit movie-recommendations-als.py 50
pyspark version:  2.1.1+hadoop2.7
Python version:  3.5.2 |Enthought, Inc. (x86_64)| (default, Mar  2 2017, 16:37:47) [MSC v.1900 64 bit (AMD64)]

Loading movie names...

Training recommendation model...

Ratings for user ID 50:
Chasing Amy (1997): 3.0
Mulholland Falls (1996): 3.0
Pillow Book, The (1995): 5.0
Trainspotting (1996): 5.0
Anne Frank Remembered (1995): 5.0
English Patient, The (1996): 2.0
Dead Man Walking (1995): 4.0
Phenomenon (1996): 2.0
Frighteners, The (1996): 4.0
Crash (1996): 1.0
People vs. Larry Flynt, The (1996): 5.0
Scream (1996): 4.0
Everyone Says I Love You (1996): 5.0
Lost Highway (1997): 5.0
Leaving Las Vegas (1995): 2.0
I Shot Andy Warhol (1996): 5.0
Basquiat (1996): 5.0
Chasing Amy (1997): 4.0
Things to Do in Denver when You're Dead (1995): 4.0
Mr. Holland's Opus (1995): 2.0
Cop Land (1997): 3.0
Lone Star (1996): 1.0
Young Poisoner's Handbook, The (1995): 4.0
Fargo (1996): 2.0

Top 10 recommendations:
Naked (1993) score 14.09516885550284
Ruby in Paradise (1993) score 13.087311090342853
Alphaville (1965) score 11.922241904473463
Addiction, The (1995) score 10.888480029856169
In the Mouth of Madness (1995) score 10.565658369334749
World of Apu, The (Apur Sansar) (1959) score 10.282155275248897
Burnt By the Sun (1994) score 9.994042546052055
Out to Sea (1997) score 9.680666841372604
Bad Taste (1987) score 9.325253655691458
Night of the Living Dead (1968) score 9.254077332335743


(User) C:\SparkCourse>spark-submit movie-recommendations-als.py 50
pyspark version:  2.1.1+hadoop2.7
Python version:  3.5.2 |Enthought, Inc. (x86_64)| (default, Mar  2 2017, 16:37:47) [MSC v.1900 64 bit (AMD64)]

Loading movie names...

Training recommendation model...

Ratings for user ID 50:
Chasing Amy (1997): 3.0
Mulholland Falls (1996): 3.0
Pillow Book, The (1995): 5.0
Trainspotting (1996): 5.0
Anne Frank Remembered (1995): 5.0
English Patient, The (1996): 2.0
Dead Man Walking (1995): 4.0
Phenomenon (1996): 2.0
Frighteners, The (1996): 4.0
Crash (1996): 1.0
People vs. Larry Flynt, The (1996): 5.0
Scream (1996): 4.0
Everyone Says I Love You (1996): 5.0
Lost Highway (1997): 5.0
Leaving Las Vegas (1995): 2.0
I Shot Andy Warhol (1996): 5.0
Basquiat (1996): 5.0
Chasing Amy (1997): 4.0
Things to Do in Denver when You're Dead (1995): 4.0
Mr. Holland's Opus (1995): 2.0
Cop Land (1997): 3.0
Lone Star (1996): 1.0
Young Poisoner's Handbook, The (1995): 4.0
Fargo (1996): 2.0

Top 10 recommendations:
Audrey Rose (1977) score 12.341288805789775
Ruby in Paradise (1993) score 11.462581688064152
Village of the Damned (1995) score 11.375580811570458
Kicking and Screaming (1995) score 11.076936336547282
April Fool's Day (1986) score 10.404393540951727
Gone Fishin' (1997) score 9.778842099632657
Cabin Boy (1994) score 9.778075775247485
Lord of Illusions (1995) score 9.745727531688262
Stupids, The (1996) score 9.677468992314225
Friday (1995) score 9.554770072401235
"""