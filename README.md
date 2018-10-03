# FrankKanesTamingBigDatawithApacheSparkandPython
My Clone Repo of source code of book Frank Kane's Taming Big Data with Apache Spark and Python

Run PySpark from Windows CLI
C:\Users\leo.liu>pyspark
Python 3.7.0b4 (v3.7.0b4:eb96c37699, May  2 2018, 19:02:22) [MSC v.1913 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/09/29 14:13:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/09/29 14:13:27 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.1.1
      /_/

Using Python version 3.7.0b4 (v3.7.0b4:eb96c37699, May  2 2018 19:02:22)
SparkSession available as 'spark'.
>>> print(sc)
<pyspark.context.SparkContext object at 0x000001EB4D24DDA0>
>>> rdd = sc.parallelize([1,2,3,4])
>>> a = rdd.map(lambda x:x*x)

>>> from pyspark import SparkConf, SparkContext
>>> import collections
>>> print (sc)
<pyspark.context.SparkContext object at 0x0000018AD5B55320>
>>> rdd = sc.parallelize([1,2,3,4])
>>> rdd.map(lambda x:x*x)
PythonRDD[1] at RDD at PythonRDD.scala:48
