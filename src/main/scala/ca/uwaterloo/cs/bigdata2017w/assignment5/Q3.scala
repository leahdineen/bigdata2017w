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

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q3 {
  val log = Logger.getLogger(getClass().getName())

  // select l_orderkey, p_name, s_name from lineitem, part, supplier
  // where
  //   l_partkey = p_partkey and
  //   l_suppkey = s_suppkey and
  //   l_shipdate = 'YYYY-MM-DD'
  // order by l_orderkey asc limit 20;
  def main(argv: Array[String]) {
    val args = new Q3Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q3")
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

    val partRDD: RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/part.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val partDF = sparkSession.read.parquet(args.input() + "/part")
        partDF.rdd.map(line => {line.mkString("|")})
      } 
    }

    val supplierRDD: RDD[String] = { 
      if (args.text()) {
        sc.textFile(args.input() + "/supplier.tbl")
      }         
      else {
        val sparkSession = SparkSession.builder.getOrCreate
        val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
        supplierDF.rdd.map(line => {line.mkString("|")})
      } 
    }

    val parts = partRDD
      .map(line => {
        var cols = line.split('|')
        // (part key, part name)
        (cols(0), cols(1))
      })
    val partKeyToPartName = sc.broadcast(parts.collectAsMap())

    val suppliers = supplierRDD
      .map(line => {
        var cols = line.split('|')
        // (supplier key, supplier name)
        (cols(0), cols(1))
      })
    val supplierKeyToSupplierName = sc.broadcast(suppliers.collectAsMap())

    val orderKeysByDate = lineItemRDD
      .flatMap(line => {
        var keys = MutableList[(Int, (String, String))]()
        var cols = line.split('|')
        // ship date is index 10
        if (cols(10).contains(targetDate)) {
          val partName = partKeyToPartName.value.get(cols(1)).get
          val supplierName = supplierKeyToSupplierName.value.get(cols(2)).get
          keys += (cols(0).toInt -> (partName, supplierName))
        }
        keys
      })
      .takeOrdered(20)(Ordering[Int].on(x => x._1))
      .foreach(x => {
        // output: (l_orderkey,p_name,s_name)
        println((x._1, x._2._1, x._2._2))
      })
  }
}

