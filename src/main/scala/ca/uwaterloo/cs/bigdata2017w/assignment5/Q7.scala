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

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q7 {
  val log = Logger.getLogger(getClass().getName())

  // select
  //   c_name,
  //   l_orderkey,
  //   sum(l_extendedprice*(1-l_discount)) as revenue,
  //   o_orderdate,
  //   o_shippriority
  // from customer, orders, lineitem
  // where
  //   c_custkey = o_custkey and
  //   l_orderkey = o_orderkey and
  //   o_orderdate < "YYYY-MM-DD" and
  //   l_shipdate > "YYYY-MM-DD"
  // group by
  //   c_name,
  //   l_orderkey,
  //   o_orderdate,
  //   o_shippriority
  // order by
  //   revenue desc
  // limit 10;
  def main(argv: Array[String]) {
    val args = new Q7Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q7")
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

    val customer = customerRDD
      .map(line => {
        var cols = line.split('|')
        // (customer key, customer name)
        (cols(0), cols(1))
      })
    val customerKeyName = sc.broadcast(customer.collectAsMap())

    val orders = ordersRDD
      .filter(line => {
        var cols = line.split('|')
        // order date
        cols(4).compareTo(targetDate) < 0
      })
      .map(line => {
        var cols = line.split('|')
        // (order key, customer key, order date, ship priority)
        (cols(0), (cols(1), cols(4), cols(7)))
      })

    val lineItems = lineItemRDD
    .filter(line => {
        var cols = line.split('|')
        // ship date
        cols(10).compareTo(targetDate) > 0
    })
    .map(line => {
      var cols = line.split('|')
      // extended price * (1 - discount)
      var revenue = cols(5).toDouble * (1.0 - cols(6).toDouble)
      // (order key, revenue)
      (cols(0), revenue)
    })
    .cogroup(orders)
    .flatMap(group => {
      var merged = MutableList[((String, String, String, String), Double)]()
      var order = group._2._2
      var lineitem = group._2._1

      for(l <- lineitem) {
        for(o <- order) {
          var key = (
            customerKeyName.value.get(o._1).get, // customer name
            group._1, // order key
            o._2, // order date
            o._3 // ship priority
          )
          // revenue
          var value = l
          merged += (key -> value)
        }
      }
      merged
    })
    .reduceByKey(_ + _)
    .takeOrdered(10)(Ordering[Double].reverse.on(x => x._2))
    .foreach(x => {
      println("(" + x._1._1 + "," + x._1._2 + "," + x._2 + "," + x._1._3 + "," + x._1._4 + ")")
    })

  }
}