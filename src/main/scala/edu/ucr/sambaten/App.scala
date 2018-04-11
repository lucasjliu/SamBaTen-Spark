package edu.ucr.sambaten

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, Level}

object App {
  def logger = Logger.getLogger(getClass.getName)
  def runTest[R](name: String, block: => R) = {
    logger.error(s"Test $name")
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime
    val etime = (t1-t0)/1e9
    logger.error(s"result: $result, time cost: $etime")
  }
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("App")
    implicit val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    new TestCoordinate
    new TestVector
    new TestMatrix
    //new TestCPALS
    new TestSambaten
    logger.error("All tests passed")
  }
}