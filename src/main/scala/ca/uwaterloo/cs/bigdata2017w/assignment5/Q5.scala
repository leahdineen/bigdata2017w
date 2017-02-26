package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import tl.lin.data.pair.PairOfStrings
import scala.math.Ordering

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
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

    val customerRDD: org.apache.spark.rdd.RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/customer.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val customerDF = sparkSession.read.parquet(args.input() + "/customer")
        customerDF.rdd.map(line => {line.mkString("|")})
      } 
    }

    val nationRDD: org.apache.spark.rdd.RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/nation.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val nationDF = sparkSession.read.parquet(args.input() + "/nation")
        nationDF.rdd.map(line => {line.mkString("|")})
      }
    }

    val nation = nationRDD
      .map(line => {
        var cols = line.split('|')
        (cols(0), cols(1))
      })
      .reduceByKey(_ + _)
    val nationKeyToNationName = sc.broadcast(nation.collectAsMap())

    val customer = customerRDD
      .map(line => {
        var cols = line.split('|')
        (cols(0), cols(3))
      })
      .reduceByKey(_ + _)
    val customerKeyToNationKey = sc.broadcast(customer.collectAsMap())

    val orderKeysByDate = lineItemRDD
      .map(line => {
        var cols = line.split('|')
        var month = cols(10).slice(0, 7)
        (cols(0), month)
      })

    val orderKeyToNation = ordersRDD
      .map(line => {
        var cols = line.split('|')
        val nationKey = customerKeyToNationKey.value.get(cols(1)).get
        val nationName = nationKeyToNationName.value.get(nationKey).get
        (cols(0), nationName)
      })
      .filter(x => x._2.equals("CANADA") || x._2.equals("UNITED STATES"))
      .cogroup(orderKeysByDate)
      .flatMap(group => {
        var merged = MutableList[String]()
        var nations = group._2._1
        var lineitem = group._2._2

        for(l <- lineitem) {
          for(n <- nations) {
            merged += n + "," + l
          }
        }
        merged
      })
      .map(nation => (nation, 1))
      .reduceByKey(_ + _)
      .sortByKey(true)
      .collect()
      .foreach(println)
  }
}
