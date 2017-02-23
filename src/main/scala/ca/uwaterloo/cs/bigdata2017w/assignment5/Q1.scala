package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import tl.lin.data.pair.PairOfStrings

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  // TODO: check that these work
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q1 {
  val log = Logger.getLogger(getClass().getName())

  // select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';
  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    var inputFile = args.input()

    if(args.text() && args.parquet()){
      log.error("Can't supply both text and parquet flags")
      return
    }  
    if (args.text()){
      inputFile += "/lineitem.tbl"
    } else if (args.text()) {
      inputFile += "/lineitem/part-r-00000-06ffba52-de7d-4aa9-a540-0b8fa4a96d6e.snappy.parquet"
    }

    val lineItemTable = sc.textFile(inputFile) 
    val targetDate = args.date()

    val shipDateCount = lineItemTable
      .flatMap(line => {
        var dates = MutableList[String]()
        var cols = line.split('|')
        // ship date is index 10
        if (cols(10).contains(targetDate)) {
          dates += targetDate
        }
        dates
      })
      .map(date => (date, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(ans => {
        println("ANSWER=" + ans._2)
      })

  }
}