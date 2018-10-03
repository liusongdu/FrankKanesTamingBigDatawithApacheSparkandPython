# coding=utf-8

from __future__ import print_function
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    # The config section is not necessary for my Windows and Spark environment combination
    # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName(
    # "LinearRegression").getOrCreate()
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    """
    -1.74,1.66
    1.24,-1.18
    0.29,-0.40
    """
    inputLines = spark.sparkContext.textFile("regression.txt")

    # Whatever the features are, it has to go into a dense vector.
    data = inputLines.map(lambda x: x.split(",")).map(
        lambda x: (
            float(x[0]), Vectors.dense(float(x[1]))
        )
    )
    # for _each_data in data.collect():
    #     print(_each_data)

    # Convert this RDD to a DataFrame, df, using toDF() method
    # Give each column a name
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    # Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    # Perhaps you're importing data from a real database. Or you are using structured streaming
    # to get your data.

    # Let's split our data into training data and testing data
    # Split dataframe into 2 dataseta
    # trainTest = df.randomSplit([0.5, 0.5])
    # trainingDF = trainTest[0]
    # testDF = trainTest[1]
    [trainingDF, testDF] = df.randomSplit([0.5, 0.5])

    # Now create our linear regression model, lir, with some certain parameters chosen
    #
    # regParam参数对应于λ(正则化参数)
    # elasticNetParam参数对应于α(学习率)
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Train the model using our training data
    model = lir.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all features in our test dataframe
    fullPredictions = model.transform(testDF).cache()
    # """
    # Row(label=2.62, features=DenseVector([-2.69]), prediction=1.930633178220813)
    # Row(label=2.67, features=DenseVector([-2.51]), prediction=1.801315318291897)
    # Row(label=2.76, features=DenseVector([-2.8]), prediction=2.009660759288484)
    # """

    # Extract the predictions and the "known" correct labels.

    # fullPredictions.select("prediction"):
    # """
    # Row(prediction=1.1449797270132513)
    # Row(prediction=1.0471248833109874)
    # Row(prediction=1.1939071488643833)
    # """
    # fullPredictions.select("prediction").rdd.map(lambda x: x[0]):
    # """
    # 0.9017773478368912
    # 1.100714075505509
    # 0.873357815312803
    # """

    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
        print(prediction)
        """
        actual value, predictions
        (1.5483217127598983, 2.08)
        (1.7543633235595375, 2.67)
        (1.9604049343591772, 2.76)
        """

    # Stop the session
    spark.stop()
