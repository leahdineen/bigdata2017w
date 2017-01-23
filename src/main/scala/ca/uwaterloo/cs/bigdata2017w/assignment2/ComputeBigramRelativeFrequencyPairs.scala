package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

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
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    
    // Compute the counts of individual words
    val words = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        var bigrams = List()
        if (tokens.length > 1) {
          tokens.sliding(2).map(p => {
            val first = p(0)
            s"$first, *"
        }).toList
        } else {
            bigrams
        }
      })
      .map(w => (w, 1.0f))
      .reduceByKey(_ + _)

    // Broadcast the word counts to be used in the frequency computation
    val word_dict = sc.broadcast(words.collectAsMap())
    
    // Compute the bigram frequency
    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        var bigrams = List()
        if (tokens.length > 1) {
          tokens.sliding(2).map(p => {
            val first = p(0)
            List(s"$first, *", p.mkString(", "))
          }).toList
        } else {
            bigrams
        }
      })
      .flatMap(b => {
        List(b(0), b(1))
      })
      .map(bigram => {
        val left = bigram.split(", ")(0)
        if(bigram.split(", ")(1) == "*") {
            (bigram, 1.0f)
        } else {
            val freq = 1.0f / word_dict.value.get(left).get
            (bigram, freq)
        }
      })
      .reduceByKey(_ + _)

    pairs.map(x => "(" + x._1 + ")\t" + x._2).saveAsTextFile(args.output())
  }
}