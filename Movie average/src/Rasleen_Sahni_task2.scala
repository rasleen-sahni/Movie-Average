/**
  * Created by Rasleen on 2/5/2017.
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkConf

import scala.compat.Platform.EOL

object Rasleen_Sahni_task2
{

  def main(argument: Array[String]) {

   // Creating a spark and sql context
    val sparkconf = new SparkConf().setAppName("Tag Rating").setMaster("local[1]")
    val sparkcontext = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sparkcontext)

   // Input files, rating.csv and tags.csv
    val ratings = argument(0)
    val tags = argument(1)

   // Creating dataframes from existing RDD of tags and ratings file
    val ratings1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(ratings)
    val tags1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(tags)


   // Performing joins on two dataframes and combining tags and ratings dataframes on movieId column and calculating average rating of each tag
    val new_df = tags1.join(ratings1, tags1("movieId")
      .equalTo(ratings1("movieId")))
      .selectExpr("tag", "rating").groupBy("tag").avg("rating").orderBy(desc("tag"))



   // Setting header to the joined table
    val column_names=Seq("tag", "rating_avg")

    // Final datafrmaes with tags and ratings average
    val final_df = new_df.toDF(column_names: _*)

   // Writing file to the disk
    final_df.repartition(1).write
      .option("header","true")
      .format("com.databricks.spark.csv")
      .save(argument(2))

  }

}