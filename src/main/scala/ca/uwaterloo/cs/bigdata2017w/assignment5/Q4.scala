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

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q4 {
  val log = Logger.getLogger(getClass().getName())

  // select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
  // where
  //   l_orderkey = o_orderkey and
  //   o_custkey = c_custkey and
  //   c_nationkey = n_nationkey and
  //   l_shipdate = 'YYYY-MM-DD'
  // group by n_nationkey, n_name
  // order by n_nationkey asc;
  def main(argv: Array[String]) {
    val args = new Q4Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    val targetDate = args.date()

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
        // (nation key, nation name)
        (cols(0), cols(1))
      })
    val nationKeyToNationName = sc.broadcast(nation.collectAsMap())

    val customer = customerRDD
      .map(line => {
        var cols = line.split('|')
        // (customer key, nation key)
        (cols(0), cols(3))
      })
    val customerKeyToNationKey = sc.broadcast(customer.collectAsMap())

    val orderKeysByDate = lineItemRDD
      .map(line => {
        var cols = line.split('|')
        // (order key, ship date)
        (cols(0), cols(10))
      })
      .filter(pair => pair._2.contains(targetDate))

    val orderKeyToNation = ordersRDD
      .map(line => {
        var cols = line.split('|')
        val nationKey = customerKeyToNationKey.value.get(cols(1)).get
        val nationName = nationKeyToNationName.value.get(nationKey).get
        (cols(0), (nationKey, nationName))
      })
      .cogroup(orderKeysByDate)
      .flatMap(group => {
        var merged = MutableList[(String, String)]()
        var nations = group._2._1
        var lineitem = group._2._2

        for(l <- lineitem) {
          for(n <- nations) {
            merged += n
          }
        }
        merged
      })
      .map(nation => (nation, 1))
      .reduceByKey(_ + _)
      .takeOrdered(20)(Ordering[Int].on(x => x._1._1.toInt))
      .foreach(x => {
        println("(" + x._1._1 + "," + x._1._2 + "," + x._2 + ")")
      })

    // output: (n_nationkey,n_name,count(*))

  }
}
