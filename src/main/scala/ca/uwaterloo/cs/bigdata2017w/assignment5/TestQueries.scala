package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import tl.lin.data.pair.PairOfStrings

class TestQueriesConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  verify()
}
object TestQueries {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TestQueriesConf(argv)

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkSession = SparkSession.builder.getOrCreate

    val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
    val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
    val customerDF = sparkSession.read.parquet(args.input() + "/customer")
    val nationDF = sparkSession.read.parquet(args.input() + "/nation")
    val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")


    lineitemDF.createOrReplaceTempView("lineitem")
    ordersDF.createOrReplaceTempView("orders")
    customerDF.createOrReplaceTempView("customer")
    nationDF.createOrReplaceTempView("nation")
    supplierDF.createOrReplaceTempView("supplier")
    
    val sqlDF = sqlContext.sql("""
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from lineitem
where
  l_shipdate = '1994-07-04'
group by l_returnflag, l_linestatus
    """)
    sqlDF.show()

    // spark-submit --class ca.uwaterloo.cs.bigdata2017w.assignment5.TestQueries target/bigdata2017w-0.1.0-SNAPSHOT.jar --input TPC-H-0.1-PARQUET
  }
}
