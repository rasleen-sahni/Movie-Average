/**
  * Created by Rasleen on 2/5/2017.
  */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import java.io._
import org.apache.spark.SparkContext
import scala.compat.Platform.EOL
import org.apache.spark.SparkConf


object Rasleen_Sahni_task1
{

  def main(argument: Array[String])
  {

    
    //Creating a spark context
    val sparkconf = new SparkConf().setAppName("Movie Rating").setMaster("local[1]")
    val sparkcontext = new SparkContext(sparkconf)

    //Reading a text file
    val textfile1 = sparkcontext.textFile(argument(0))

    // Filtering out the header
    val first_row = textfile1.first()
    val textfile2    = textfile1.filter(x => x != first_row)


    // Splitting the input ratings file, which is separated by commas
    val ratings = textfile2.map { row => val value = row.split(",")
      (value(1).toInt, value(2).toDouble) }

   
    val average_ratings = ratings.groupByKey()
      .map(row => { val avg = row._2.sum / row._2.size;
      (avg, row._1) }).collect().sortBy(_._2)

    

    //Writing to a file
    var writer = new FileWriter(argument(1))
 
    for ((avg,movieid) <- average_ratings)
    {
      
      writer.write(" " + movieid + " , " + avg  + " " + EOL)
      
    }
    writer.close()

    }

}




