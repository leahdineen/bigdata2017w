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
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
  val date = opt[String](descr = "date", required = true)
  // TODO: check that these work
  val text = opt[Boolean](descr = "use text data")
  val parquet = opt[Boolean](descr = "use parquet data")
  verify()
}
object Q5 {
  val log = Logger.getLogger(getClass().getName())

  // select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
  // where
  //   l_orderkey = o_orderkey and
  //   o_custkey = c_custkey and
  //   c_nationkey = n_nationkey
  // group by n_nationkey, n_name, l_shipdate = "YYYY-MM"
  // order by n_nationkey asc;
  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text File: " + args.text())
    log.info("Parquet File: " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    var lineItemFile = args.input()
    var ordersFile = args.input()
    var customerFile = args.input()
    var nationFile = args.input()

    if(args.text() && args.parquet()){
      log.error("Can't supply both text and parquet flags")
      return
    }  
    if (args.text()){
      lineItemFile += "/lineitem.tbl"
      ordersFile += "/orders.tbl"
      customerFile += "/customer.tbl"
      nationFile += "/nation.tbl"
    } else if (args.text()) {
      lineItemFile += "/lineitem/part-r-00000-06ffba52-de7d-4aa9-a540-0b8fa4a96d6e.snappy.parquet"
      ordersFile += "/orders/part-r-00000-8609b9f0-13be-469d-b826-b68d3eaaed45.snappy.parquet"
      customerFile += "/customer/part-r-00000-0443221a-257c-439e-a2c5-6c8127f0abeb.snappy.parquet"
      nationFile += "/nation/part-r-00000-9de3d941-bf39-429b-8fc2-08bdfe6e6735.snappy.parquet"
    }

    val lineItemTable = sc.textFile(lineItemFile) 
    val ordersTable = sc.textFile(ordersFile)
    val customerTable = sc.textFile(customerFile)
    val nationTable = sc.textFile(nationFile)
    val targetDate = args.date()

    val nation = nationTable
      .map(line => {
        var cols = line.split('|')
        (cols(0), cols(1))
      })
      .reduceByKey(_ + _)
    val nationKeyToNationName = sc.broadcast(nation.collectAsMap())

    val customer = customerTable
      .map(line => {
        var cols = line.split('|')
        (cols(0), cols(3))
      })
      .reduceByKey(_ + _)
    val customerKeyToNationKey = sc.broadcast(customer.collectAsMap())

    val orderKeysByDate = lineItemTable
      .map(line => {
        var cols = line.split('|')
        var month = cols(10).slice(0, 8)
        (cols(0), month)
      })

    val orderKeyToNation = ordersTable
      .map(line => {
        var cols = line.split('|')
        val nationKey = customerKeyToNationKey.value.get(cols(1)).get
        val nationName = nationKeyToNationName.value.get(nationKey).get
        (cols(0), (nationKey, nationName))
      })
      //.cogroup(orderKeysByDate)


    // output: (n_nationkey,n_name,shipdate,count(*))

  }
}
