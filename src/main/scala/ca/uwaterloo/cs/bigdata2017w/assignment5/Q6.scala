// package ca.uwaterloo.cs.bigdata2017w.assignment5

// import org.apache.spark.sql.SparkSession
// import org.apache.log4j._
// import org.apache.hadoop.fs._
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf
// import org.rogach.scallop._
// import scala.collection.mutable.MutableList
// import tl.lin.data.pair.PairOfStrings

// class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
//   mainOptions = Seq(input, date, text, parquet)
//   val input = opt[String](descr = "input path", required = true)
//   // date could be of the form YYY-MM-DD, YYYY-MM, or YYYY
//   val date = opt[String](descr = "date", required = true)
//   val text = opt[Boolean](descr = "use text data")
//   val parquet = opt[Boolean](descr = "use parquet data")
//   verify()
// }
// object Q6 {
//   val log = Logger.getLogger(getClass().getName())

// // select
// //   l_returnflag,
// //   l_linestatus,
// //   sum(l_quantity) as sum_qty,
// //   sum(l_extendedprice) as sum_base_price,
// //   sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
// //   sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
// //   avg(l_quantity) as avg_qty,
// //   avg(l_extendedprice) as avg_price,
// //   avg(l_discount) as avg_disc,
// //   count(*) as count_order
// // from lineitem
// // where
// //   l_shipdate = 'YYYY-MM-DD'
// // group by l_returnflag, l_linestatus;

//   def main(argv: Array[String]) {
//     val args = new Q6Conf(argv)

//     log.info("Input: " + args.input())
//     log.info("Date: " + args.date())
//     log.info("Text File: " + args.text())
//     log.info("Parquet File: " + args.parquet())

//     val conf = new SparkConf().setAppName("Q6")
//     val sc = new SparkContext(conf)

//     val targetDate = args.date()

//     val lineItemRDD: org.apache.spark.rdd.RDD[String] = { 
//       if (args.text()) {
//         sc.textFile(args.input() + "/lineitem.tbl")
//       }         
//       else {
//         val sparkSession = SparkSession.builder.getOrCreate
//         val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
//         lineitemDF.rdd.map(line => {line.mkString("|")})
//       } 
//     }

//     val shipDateCount = lineItemRDD
//       .filter(line => {
//         var cols = line.split('|')
//         // (order key, ship date)
//         cols(10).contains(targetDate)
//       })

//   }
// }Ï