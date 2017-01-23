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


class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer{
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}
object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    textFile.cache

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

    // Broadcast the word counts to be used in the frequency computation
    val word_dict = sc.broadcast(words.collectAsMap())

    // Compute the Pairs PMI
    val pairs_pmi = textFile
      .flatMap(line => {
        var pairs = mutable.ListBuffer[String]()
        val tokens = tokenize(line)
        val loop_size = math.min(tokens.length, context_size)
        var a = 0
        var b = 0
        // loop over input building the pairs
        for (a <- 0 until loop_size) {
          var t1 = tokens(a)
          for (b <- 0 until loop_size) {
            var t2 = tokens(b)
            if(!(t1 == t2)) {
              val pair = t1 + " " + t2
              if (!(pairs.contains(pair))) {
                // only add distinct pairs
                pairs += pair
              }
            }
          }
        }
        pairs
      })
      .map(p => (p, 1))
      .reduceByKey(_ + _)
      .flatMap(p => {
        var output = new mutable.ListBuffer[String]()

        val left = p._1.split(" ")(0)
        val right = p._1.split(" ")(1)
        val pair_count = p._2

        //threshold?
        if (pair_count >= 10) {
          val p_x_and_y = pair_count / total_lines.value.toFloat
          val p_x = word_dict.value.get(left).get / total_lines.value.toFloat
          val p_y = word_dict.value.get(right).get / total_lines.value.toFloat

          val pmi = (math.log(p_x_and_y / (p_x * p_y)) / math.log(10)).toFloat

          output += "(" + left + ", " + right + ")\t(" + pmi + ", " + pair_count + ")"
        }
        output
      })
      .saveAsTextFile(args.output())
  }
}