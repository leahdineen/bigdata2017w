package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import tl.lin.data.pair.PairOfStrings


class ConfPairs(args: Seq[String]) extends ScallopConf(args) with Tokenizer{
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}
object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    
    // Compute the counts of individual words
    val words = textFile
      .flatMap(line => tokenize(line))
      .map(w => (w, 1))
      .reduceByKey(_ + _)

    // Broadcast the word counts to be used in the frequency computation
    val word_dict = sc.broadcast(words.collectAsMap())
    
    // Compute the bigram frequency
    val stripes = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(", ")).toList else List()
      })
      .map(bigram => {
        val left = bigram.split(", ")(0)
        val freq = 1f / word_dict.value.get(left).get
        (bigram, freq)
      })
      .reduceByKey(_ + _)

    stripes.saveAsTextFile(args.output())
    words.saveAsTextFile("words_output")
  }
}