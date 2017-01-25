package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable
import scala.math
import tl.lin.data.pair.PairOfStrings


class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer{
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "co-occurence threshold", required = false, default = Some(1))
  verify()
}
object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Compute Stripes PMI")
    conf.set("spark.default.parallelism", args.reducers().toString)
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    textFile.cache

    val threshold = args.threshold()
    val context_size = 40
    // count the number of lines in the input RDD
    val lines = textFile.count()
    val total_lines = sc.broadcast(lines)

    // Compute the counts of individual words
    val words = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val loop_size = math.min(tokens.length, context_size)
        var distinct = new mutable.ListBuffer[String]()
        var a = 0

        for (a <- 0 until loop_size) {
          var t = tokens(a)
          if(!(distinct.contains(t))) {
            distinct += t
          }
        }
        distinct
      })
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(w => w._2 >= threshold)

    // Broadcast the word counts to be used in the frequency computation
    val word_dict = sc.broadcast(words.collectAsMap())

    // Compute the Stripe PMI
    val stripes_pmi = textFile
      .flatMap(line => {
        var stripes = mutable.Map[String, mutable.Map[String, Int]]()
        val tokens = tokenize(line)
        val loop_size = math.min(tokens.length, context_size)
        var a = 0
        var b = 0
        // loop over input building the pairs
        for (a <- 0 until loop_size) {
          var t1 = tokens(a)
          var stripe = mutable.Map[String, Int]()
          if (stripes.contains(t1)) {
            stripe = stripes(t1)
          }

          for (b <- 0 until loop_size) {
            var t2 = tokens(b)
            
            if(!(t1 == t2)) {
              if (!(stripe.contains(t2))) {
                // only add distinct pairs
                stripe += (t2 -> 1)
              }
            }
          }
          stripes += (t1 -> stripe)
        }
        stripes
      })
      .reduceByKey((x, y) => {
        y.foreach(kv => {
          if (x contains kv._1) {
            var count = x(kv._1) + kv._2
            x += (kv._1 -> count)
          } else {
            x += (kv._1 -> kv._2)
          }        
        })
        x
      })
      .flatMap(x => {
        var output = new mutable.ListBuffer[String]()
        var str = new mutable.ListBuffer[String]()

        x._2.foreach(kv => {
          val pair_count = kv._2

          if (pair_count >= threshold) {
            val p_x_and_y = pair_count / total_lines.value.toFloat
            val p_x = word_dict.value.get(x._1).get / total_lines.value.toFloat
            val p_y = word_dict.value.get(kv._1).get / total_lines.value.toFloat

            val pmi = (math.log(p_x_and_y / (p_x * p_y)) / math.log(10)).toFloat

            str += kv._1 + "=(" + pmi + ", " + pair_count + ")"
          }
        })
        if (str.length != 0) {
          output += x._1 + "\t{" + str.mkString(", ") + "}"
        }
        output
      })
      .saveAsTextFile(args.output())
  }
}