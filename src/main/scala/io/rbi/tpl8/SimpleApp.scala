package io.rbi.tpl8

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * example spark app
  */
object SimpleApp {
  def main(args: Array[String]) {
    // configure spark instances
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("Simple Application")
        .setMaster("local[2]")
    )
    // simulated log data
    val logData = "login crash login warning crash low-memory"
    val df = sc.parallelize( logData.split(" ") ).cache()
    // count crashes
    val crashCount = df.filter(line => line.contains("crash")).count()
    println(s"Number of crashes in log: ${crashCount}\n")
    sc.stop()
  }
}