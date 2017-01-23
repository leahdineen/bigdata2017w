package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable
import tl.lin.data.pair.PairOfStrings


class ComputeBigramRelativeFrequencyStripesConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer{
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}
object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ComputeBigramRelativeFrequencyStripesConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    textFile.cache
    
    // Compute the bigram frequency
    val stripes = textFile
      .flatMap(line => {
        var stripes = mutable.Map[String, mutable.Map[String, Float]]()
        val tokens = tokenize(line)
        var a = 0
        
        // loop over pairs of adjacent tokens
        for ( a <- 1 until tokens.length) {
          var prev = tokens(a-1)
          var curr = tokens(a)
          
          // add pair to map
          if (stripes contains prev) {
            var stripe = stripes(prev)
            if (stripe contains curr) {
              var count = stripe(curr) + 1.0f
              stripe += (curr -> count)
            } else {
              stripe += (curr -> 1.0f)
            }
          } else {
            var stripe = mutable.Map[String, Float](curr -> 1.0f)
            stripes put (prev, stripe)
          }
        }
        stripes
      })
      .reduceByKey((x, y) => {
        // element wise sum of map
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
      .map(x => {
        var str = new mutable.ListBuffer[String]()
        // sum of all occurences
        var sum = x._2.foldLeft(0.0f)(_ + _._2)
        // calculate frequency and format into string for output
        x._2.foreach(kv => {
          str += kv._1 + "=" + kv._2 / sum
        })
        // string formatting to match the output of the bespin implementation
        x._1 + "\t{" + str.mkString(", ") + "}"
      })
      .saveAsTextFile(args.output())
  }
}