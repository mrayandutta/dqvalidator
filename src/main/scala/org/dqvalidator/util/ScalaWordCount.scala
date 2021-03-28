package org.dqvalidator.util

import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]) {
    //To set HADOOP_HOME.
    System.setProperty("hadoop.home.dir", "C:/Softwares/winutils-master/hadoop-2.6.0");
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark WordCount").setMaster("local[2]"))
    //Load inputFile
    val inputFile = sc.textFile("src/main/resources/input.txt")
    val counts = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    counts.foreach(println)
    sc.stop()
  }
}