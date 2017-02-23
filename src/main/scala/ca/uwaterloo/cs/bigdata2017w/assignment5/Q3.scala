package ca.uwaterloo.cs.bigdata2017w.assignment5

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
  // TODO: check that these work
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

    var lineItemFile = args.input()
    var partFile = args.input()
    var supplierFile = args.input()

    if(args.text() && args.parquet()){
      log.error("Can't supply both text and parquet flags")
      return
    }  
    if (args.text()){
      lineItemFile += "/lineitem.tbl"
      partFile += "/part.tbl"
      supplierFile += "/supplier.tbl"
    } else if (args.text()) {
      lineItemFile += "/lineitem/part-r-00000-06ffba52-de7d-4aa9-a540-0b8fa4a96d6e.snappy.parquet"
      partFile += "/part/part-r-00000-742ee5ef-d09d-4669-a318-8e46f2332a9c.snappy.parquet"
      supplierFile += "/supplier/part-r-00000-d15e4c0e-18ab-4d49-bfaf-7c5757fca3ac.snappy.parquet"
    }

    val lineItemTable = sc.textFile(lineItemFile) 
    val partTable = sc.textFile(partFile)
    val supplierTable = sc.textFile(supplierFile)
    val targetDate = args.date()


    val parts = partTable
      .map(line => {
        var cols = line.split('|')
        (cols(0), cols(1))
      })
      .reduceByKey(_ + _)
    val partKeyToPartName = sc.broadcast(parts.collectAsMap())

    val suppliers = supplierTable
      .map(line => {
        var cols = line.split('|')
        (cols(0), cols(1))
      })
      .reduceByKey(_ + _)
    val supplierKeyToSupplierName = sc.broadcast(suppliers.collectAsMap())

    // val partKeyToOrderKey = lineItemTable
    //   .map(line => {
    //     var cols = line.split('|')
    //     (cols(1), cols(0))
    //   })
    //   .reduceByKey(_ + _)

    // val supplierKeyToOrderKey = lineItemTable
    //   .map(line => {
    //     var cols = line.split('|')
    //     (cols(2), cols(0))
    //   })
    //   .reduceByKey(_ + _)

    val orderKeysByDate = lineItemTable
      .flatMap(line => {
        var keys = MutableList[String]()
        var cols = line.split('|')
        // ship date is index 10
        if (cols(10).contains(targetDate)) {
          val partName = partKeyToPartName.value.get(cols(1)).get
          val supplierName = supplierKeyToSupplierName.value.get(cols(2)).get
          keys += cols(0) + '|' + partName + '|' + supplierName
        }
        keys
      })
      .map(key => (key, 1))
      .reduceByKey(_ + _)
      .map(group => {
        val s = group._1.split('|')
        (s(0), (s(1), (s(2))))
      })
      .sortByKey(true)
      .collect()
      .take(20)
      .foreach(ans => {
        // output: (l_orderkey,p_name,s_name)
        println((ans._1, ans._2._1, ans._2._2))
      })
  }
}

