package ca.uwaterloo.cs.bigdata2017w.assignment5

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
  // TODO: check that these work
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
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    var lineItemFile = args.input()
    var ordersFile = args.input()

    if(args.text() && args.parquet()){
      log.error("Can't supply both text and parquet flags")
      return
    }  
    if (args.text()){
      lineItemFile += "/lineitem.tbl"
      ordersFile += "/orders.tbl"
    } else if (args.text()) {
      lineItemFile += "/lineitem/part-r-00000-06ffba52-de7d-4aa9-a540-0b8fa4a96d6e.snappy.parquet"
      ordersFile += "/orders/part-r-00000-8609b9f0-13be-469d-b826-b68d3eaaed45.snappy.parquet"
    }

    val lineItemTable = sc.textFile(inputFile) 
    val orderTable = sc.textFile(inputFile)
    val targetDate = args.date()

    val orderKeys = lineItemTable
      .flatMap(line => {
        var keys = MutableList[String]()
        var cols = line.split('|')
        // ship date is index 10
        if (cols(10).contains(targetDate)) {
            keys += cols(0)
        }
        keys
      })
      .map(key => (key, 1))
      .reduceByKey(_ + _)

    val clerksByKey = orderTable
      .flatMap(line => {
        var keys = MutableList[String]()
        var cols = line.split('|')

      })
      .map(key => (key, 1))
      .reduceByKey(_ + _)
      .sortByKey(true)
      .collect()
      .take(20)
      .foreach(println)

  }
}