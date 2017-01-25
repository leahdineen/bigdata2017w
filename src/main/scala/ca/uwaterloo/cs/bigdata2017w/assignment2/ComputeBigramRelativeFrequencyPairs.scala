package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import tl.lin.data.pair.PairOfStrings


class ComputeBigramRelativeFrequencyPairsConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer{
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}
object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ComputeBigramRelativeFrequencyPairsConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Pairs")
    conf.set("spark.default.parallelism", args.reducers().toString)
    val sc = new SparkContext(conf)


    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())


    // Compute the bigram frequency
    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        var bigrams = MutableList[String]()
        if (tokens.length > 1) {
          var a = 0
          // loop over pairs of adjacent tokens
          for ( a <- 1 until tokens.length) {
            var prev = tokens(a-1)
            var curr = tokens(a)
            bigrams += prev + ", *"
            bigrams += prev + ", " + curr
          }
        }
        bigrams
      })
      .map(bigram => (bigram, 1.0f))
      .reduceByKey(_ + _)
      .sortByKey(true)
      .groupBy(bigram => bigram._1.split(", ")(0))
      .flatMap(bigrams => {
        var output = MutableList[(String, Float)]()
        var marginal = 1.0f;
        for(bigram <- bigrams._2) {
          val left = bigram._1.split(", ")(0)
          val right = bigram._1.split(", ")(1)
          if(right == "*") {
            marginal = bigram._2
            output += bigram
          } else {
            val freq = bigram._2 / marginal
            output += (bigram._1 -> freq.toFloat)
          }
        }
        output
      })
      .map(x => "(" + x._1 + ")\t" + x._2)
      .saveAsTextFile(args.output())
  }
}