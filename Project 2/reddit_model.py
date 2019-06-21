#!/usr/bin/env python3
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import ArrayType, StringType
import os.path
import cleantext

# Bunch of imports (may need more)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

# TASK 4
# Code for task 4
def makeNGrams(body):
    list = cleantext.sanitize(body)
    return list

# TASK 5
# Code for task 5
def makeStringArr(list):
    resList= []
    i = 1
    while i < len(list):
        resList += list[i].split(' ')
        i += 1

    return resList

# TASK 9
# Code for task 9
def get_poslabel(prob):
    if prob[1] > 0.2:
        return 1
    else:
        return 0

def get_neglabel(prob):
    if prob[1] > 0.25:
        return 1
    else:
        return 0

# TASK 10
# Code for task 10
def isState(text):
    if text in states:
        return True
    else:
        return False

def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    # TASK 1
    # Code for task 1
    # Load the parquet files
    if os.path.exists("./comments.parquet"):
        commentsDF = context.read.parquet("comments.parquet")
    else:
        commentsDF = context.read.json("comments-minimal.json.bz2")
        commentsDF.write.parquet("comments.parquet")

    if os.path.exists("./submissions.parquet"):
        submissionsDF = context.read.parquet("submissions.parquet")
    else:
        submissionsDF = context.read.json("submissions.json.bz2")
        submissionsDF.write.parquet("submissions.parquet")

    if os.path.exists("./labeled_data.parquet"):
        labeledDataDF = context.read.parquet("labeled_data.parquet")
    else:
        labeledDataDF = context.read.csv("labeled_data.csv")
        labeledDataDF.write.parquet("labeled_data.parquet")

    commentsDF.createOrReplaceTempView("commentsDF")
    submissionsDF.createOrReplaceTempView("submissionsDF")
    labeledDataDF.createOrReplaceTempView("labeledDataDF")

    #register our UDF's
    context.udf.register("makeNGrams", makeNGrams)
    context.udf.register("makeStringArr", makeStringArr, ArrayType(StringType()))

    # TASKS 2, 4, 5
    # Code for tasks 2, 4, and 5
    if os.path.exists("./spark-warehouse/joineddf"):
        joinedDF = context.read.load("./spark-warehouse/joineddf")
    else:
        joinedDF = context.sql("""
            SELECT 
                makeStringArr(makeNGrams(body)) AS strArr,
                _c3 AS trump,
                CASE
                    WHEN _c3=1 THEN 1
                    ELSE 0
                END AS poslabel,
                CASE
                    WHEN _c3=-1 THEN 1
                    ELSE 0
                END AS neglabel
            FROM 
                (
                    SELECT 
                        id,
                        body 
                    FROM commentsDF
                ) comments
                JOIN
                (
                    SELECT 
                        _c0,
                        _c3
                    FROM labeledDataDF
                ) labeledData
                ON labeledData._c0 = comments.id
            """)
        joinedDF.write.saveAsTable("joineddf")

    # TASK 6A
    # Code for task 6A
    cv = CountVectorizer(inputCol="strArr", outputCol="features", minDF=10.0)
    model = cv.fit(joinedDF)

    if os.path.exists("./spark-warehouse/trainingdf"):
        trainingDF = context.read.load("./spark-warehouse/trainingdf")
    else:
        trainingDF = model.transform(joinedDF)
        trainingDF.write.saveAsTable("trainingDF")

    trainingDF.createOrReplaceTempView("trainingDF")

    # TASK 6B
    # Code for task 6B
    if os.path.exists("./spark-warehouse/posdf"):
        pos = context.read.load("./spark-warehouse/posdf")
    else:
        pos = context.sql("""
            SELECT
                poslabel,
                neglabel,
                features
            FROM trainingdf
            """)
        pos.write.saveAsTable("posdf")


    if os.path.exists("./spark-warehouse/negdf"):
        neg = context.read.load("./spark-warehouse/negdf")
    else:
        neg = context.sql("""
            SELECT
                poslabel,
                neglabel,
                features
            FROM trainingdf
            """)
        neg.write.saveAsTable("negdf")

    # TASK 7
    # Code for task 7
    if not os.path.exists("./project2/pos.model") and not os.path.exists("./project2/neg.model"):
        # Initialize two logistic regression models.
        # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
        poslr = LogisticRegression(labelCol="poslabel", featuresCol="features", maxIter=10)
        neglr = LogisticRegression(labelCol="neglabel", featuresCol="features", maxIter=10)
        # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
        posEvaluator = BinaryClassificationEvaluator(labelCol="poslabel")
        negEvaluator = BinaryClassificationEvaluator(labelCol="neglabel")
        # There are a few parameters associated with logistic regression. We do not know what they are a priori.
        # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
        # We will assume the parameter is 1.0. Grid search takes forever.
        posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
        negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
        # We initialize a 5 fold cross-validation pipeline.
        posCrossval = CrossValidator(
            estimator=poslr,
            evaluator=posEvaluator,
            estimatorParamMaps=posParamGrid,
            numFolds=5)
        negCrossval = CrossValidator(
            estimator=neglr,
            evaluator=negEvaluator,
            estimatorParamMaps=negParamGrid,
            numFolds=5)
        # Although crossvalidation creates its own train/test sets for
        # tuning, we still need a labeled test set, because it is not
        # accessible from the crossvalidator (argh!)
        # Split the data 50/50
        posTrain, posTest = pos.randomSplit([0.5, 0.5])
        negTrain, negTest = neg.randomSplit([0.5, 0.5])
        # Train the models
        print("Training positive classifier...")
        posModel = posCrossval.fit(posTrain)
        print("Training negative classifier...")
        negModel = negCrossval.fit(negTrain)

        # Once we train the models, we don't want to do it again. We can save the models and load them again later.
        posModel.save("project2/pos.model")
        negModel.save("project2/neg.model")
    else:
        posModel = CrossValidatorModel.load("./project2/pos.model")
        negModel = CrossValidatorModel.load("./project2/neg.model")


    # TASKS 8
    # Code for tasks 8 and 9
    if os.path.exists("./spark-warehouse/finaldf"):
        finalDF = context.read.load("./spark-warehouse/finaldf")
    else:
        finalDF = context.sql("""
            SELECT
                C.id,
                S.title,
                C.author_flair_text,
                C.created_utc,
                makeStringArr(makeNGrams(C.body)) AS strArr
            FROM commentsDF C JOIN submissionsDF S ON REPLACE(C.link_id, 't3_', '') = S.id
            WHERE C.body NOT LIKE '%/s%' OR C.body NOT LIKE '&gt%'
            """)
        finalDF.write.saveAsTable("finaldf")

    # TASK 9
    # Code for task 9
    if os.path.exists("./spark-warehouse/posresult") and os.path.exists("./spark-warehouse/negresult"):
        posResult = context.read.load("./spark-warehouse/posresult")
        negResult = context.read.load("./spark-warehouse/negresult")
    else:
        dfWithCV = model.transform(finalDF)
        posResult = posModel.transform(dfWithCV)
        negResult = negModel.transform(dfWithCV)
        posResult.write.saveAsTable("posresult")
        negResult.write.saveAsTable("negresult")

    posResult.createOrReplaceTempView("posresultDF")
    negResult.createOrReplaceTempView("negresultDF")

    context.udf.register("get_poslabel", get_poslabel)
    context.udf.register("get_neglabel", get_neglabel)

    if os.path.exists("./spark-warehouse/resultsdf"):
        resultsDF = context.read.load("./spark-warehouse/resultsdf")
    else:
        resultsDF = context.sql("""
            SELECT
                pos.id AS id,
                CASE
                    WHEN get_poslabel(pos.probability) = 1 THEN 1
                    ELSE 0
                END AS pos_label,
                CASE
                    WHEN get_neglabel(neg.probability) = 1 THEN 1
                    ELSE 0
                END AS neg_label
            FROM posresultDF pos
            JOIN negresultDF neg
                ON pos.id = neg.id
            """)
        resultsDF.write.saveAsTable("resultsdf")

    resultsDF.createOrReplaceTempView("resultsDF")

    # TASK 10
    # Code for task 10

    # Join prediction results with necessary columns for task 10
    if os.path.exists("./spark-warehouse/final_resultsdf"):
        final_resultsDF = context.read.load("./spark-warehouse/final_resultsdf")
    else:
        final_resultsDF = context.sql("""
            SELECT
                comm.id AS comment_id,
                sub.id AS submission_id,
                comm.author_flair_text AS state,
                comm.created_utc AS unix_time,
                comm.score AS comment_score,
                sub.score AS story_score,
                res.pos_label as pos_label,
                res.neg_label as neg_label
            FROM resultsDF res
            JOIN commentsDF comm
                ON res.id = comm.id
            JOIN submissionsDF sub
                ON REPLACE(comm.link_id, 't3_', '') = sub.id
            """)
        final_resultsDF.write.saveAsTable("final_resultsdf")

    final_resultsDF.createOrReplaceTempView("final_resultsDF")

    # Compute percentage of positive/negative comments
    # (1) Across all submissions/posts
    if os.path.exists("./spark-warehouse/across_subs"):
        across_subsDF = context.read.load("./spark-warehouse/across_subs")
    else:
        across_subsDF = context.sql("""
            SELECT
                submission_id,
                AVG(pos_label) AS pos_percent,
                AVG(neg_label) AS neg_percent
            FROM final_resultsDF
            GROUP BY submission_id
            """)
        across_subsDF.write.saveAsTable("across_subs")
        across_subsDF.repartition(1).write.csv("project2/across_subs.csv", mode='overwrite', header='true')

    # (2) Across all days
    if os.path.exists("./spark-warehouse/across_days"):
        across_daysDF = context.read.load("./spark-warehouse/across_days")
    else:
        across_daysDF = context.sql("""
            SELECT
                from_unixtime(unix_time, 'yyyy-MM-dd') AS date,
                AVG(pos_label) AS pos_percent,
                AVG(neg_label) AS neg_percent
            FROM final_resultsDF
            GROUP BY from_unixtime(unix_time, 'yyyy-MM-dd')
            """)
        across_daysDF.write.saveAsTable("across_days")
        across_daysDF.repartition(1).write.csv("project2/across_days.csv", mode='overwrite', header='true')

    # (3) Across all states
    if os.path.exists("./spark-warehouse/across_states"):
        across_statesDF = context.read.load("./spark-warehouse/across_states")
    else:
        context.udf.register("isState", isState)
        across_statesDF = context.sql("""
            SELECT
                state,
                AVG(pos_label) AS pos_percent,
                AVG(neg_label) AS neg_percent
            FROM final_resultsDF
            WHERE isState(state) = True
            GROUP BY state
            """)
        across_statesDF.write.saveAsTable("across_states")
        across_statesDF.repartition(1).write.csv("project2/across_states.csv", mode='overwrite', header='true')

    # (4) Across comment and story scores
    if os.path.exists("./spark-warehouse/comm_scores"):
        comm_scoresDF = context.read.load("./spark-warehouse/comm_scores")
    else:
        comm_scoresDF = context.sql("""
            SELECT
                comment_score,
                AVG(pos_label) AS pos_percent,
                AVG(neg_label) AS neg_percent
            FROM final_resultsDF
            GROUP BY comment_score
            """)
        comm_scoresDF.write.saveAsTable("comm_scores")
        comm_scoresDF.repartition(1).write.csv("project2/comm_scores.csv", mode='overwrite', header='true')
    
    if os.path.exists("./spark-warehouse/story_scores"):
        story_scoresDF = context.read.load("./spark-warehouse/story_scores")
    else:
        story_scoresDF = context.sql("""
            SELECT
                story_score,
                AVG(pos_label) AS pos_percent,
                AVG(neg_label) AS neg_percent
            FROM final_resultsDF
            GROUP BY story_score
            """)
        story_scoresDF.write.saveAsTable("story_scores")
        story_scoresDF.repartition(1).write.csv("project2/story_scores.csv", mode='overwrite', header='true')

    # List of top 10 positive/negative stories
    across_subsDF.createOrReplaceTempView("across_subsDF")
    if os.path.exists("./spark-warehouse/story_list"):
        story_listDF = context.read.load("./spark-warehouse/story_list")
    else:
        story_listDF = context.sql("""
            SELECT
                submission_id,
                title,
                pos_percent,
                neg_percent
            FROM across_subsDF X
            JOIN submissionsDF Y
                ON X.submission_id = Y.id
            """)
        story_listDF.write.saveAsTable("story_list")

    story_listDF.repartition(1).orderBy("pos_percent", ascending = False).limit(10).write.csv("project2/top_pos_stories.csv", mode='overwrite', header='true')
    story_listDF.repartition(1).orderBy("neg_percent", ascending = False).limit(10).write.csv("project2/top_neg_stories.csv", mode='overwrite', header='true')

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    sc.setLogLevel("WARN")
    main(sqlContext)
