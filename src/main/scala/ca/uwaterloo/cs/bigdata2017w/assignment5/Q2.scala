package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import tl.lin.data.pair.PairOfStrings

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q2 {
  val log = Logger.getLogger(getClass().getName())

  // select o_clerk, o_orderkey from lineitem, orders
  // where
  //   l_orderkey = o_orderkey and
  //   l_shipdate = 'YYYY-MM-DD'
  // order by o_orderkey asc limit 20;
  def main(argv: Array[String]) {
    val args = new Q2Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val lineItemRDD: org.apache.spark.rdd.RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/lineitem.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        lineitemDF.rdd.map(line => {line.mkString("|")})
      } 
    }

    val ordersRDD: org.apache.spark.rdd.RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/orders.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        ordersDF.rdd.map(line => {line.mkString("|")})
      } 
    }

    val targetDate = args.date()

    val orderKeys = lineItemRDD
      .map(line => {
        var cols = line.split('|')
        // (order key, ship date)
        (cols(0), cols(10))
      })
      .filter(pair => pair._2.contains(targetDate))

    val clerksByKey = ordersRDD
      .map(line => {
        var cols = line.split('|')
        // (order key, clerk)
        (cols(0), cols(6))
      })
      .cogroup(orderKeys)
      .flatMap(group => {
        var merged = MutableList[(Int, String)]()
        var clerks = group._2._1
        var lineitem = group._2._2

        for(l <- lineitem) {
          for(c <- clerks) {
            merged += (group._1.toInt -> c)
          }
        }
        merged
      })
      .takeOrdered(20)(Ordering[Int].on(x => x._1))
      .foreach(ans => {
        // output: (o_clerk,o_orderkey)
        println((ans._2, ans._1))
      })

  }
}