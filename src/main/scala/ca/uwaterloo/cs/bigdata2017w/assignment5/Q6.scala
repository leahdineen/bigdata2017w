package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import tl.lin.data.pair.PairOfStrings

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q6 {
  val log = Logger.getLogger(getClass().getName())

// select
//   l_returnflag,
//   l_linestatus,
//   sum(l_quantity) as sum_qty,
//   sum(l_extendedprice) as sum_base_price,
//   sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
//   sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
//   avg(l_quantity) as avg_qty,
//   avg(l_extendedprice) as avg_price,
//   avg(l_discount) as avg_disc,
//   count(*) as count_order
// from lineitem
// where
//   l_shipdate = 'YYYY-MM-DD'
// group by l_returnflag, l_linestatus;

  def main(argv: Array[String]) {
    val args = new Q6Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val targetDate = args.date()

    val lineItemRDD: RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/lineitem.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        lineitemDF.rdd.map(line => {line.mkString("|")})
      } 
    }

    val lineItemReport = lineItemRDD
      .filter(line => {
        var cols = line.split('|')
        cols(10).contains(targetDate)
      })
      .map(line => {
        var cols = line.split('|')
        var return_flag = cols(8)
        var line_status = cols(9)
        var quantity = cols(4).toDouble
        var ext_price = cols(5).toDouble
        var discount = cols(6).toDouble
        var tax = cols(7).toDouble

        var key = (return_flag, line_status)
        var value = (
          quantity, // sum_qty
          ext_price, // sum_base_price
          ext_price * (1.0 - discount), // sum_disc_price
          ext_price * (1.0 - discount) * (1.0 + tax), // sum_charge
          quantity, // avg_qty
          ext_price, // avg_price
          discount, // avg_disc
          1
        )

        (key, value)
      })
      .reduceByKey((x, y) => {
        var combined = (
          x._1 + y._1, // sum_qty
          x._2 + y._2, // sum_base_price
          x._3 + y._3, // sum_disc_price
          x._4 + y._4, // sum_charge
          x._5 + y._5, // avg_qty
          x._6 + y._6, // avg_price
          x._7 + y._7, // avg_disc
          x._8 + y._8 //count(*)
        )
        combined
      })
      .collect()
      .foreach(x => {
        var output = (
          x._1._1,
          x._1._2,
          x._2._1,
          x._2._2,
          x._2._3,
          x._2._4,
          x._2._5 / x._2._8,
          x._2._6 / x._2._8,
          x._2._7 / x._2._8,
          x._2._8
        )
        println(output)
      })

  }
}