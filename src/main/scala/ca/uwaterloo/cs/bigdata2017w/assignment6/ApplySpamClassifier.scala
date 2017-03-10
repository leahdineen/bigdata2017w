package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import scala.collection.Map
import scala.math

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}
object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // Scores a document based on its list of features.
  def spamminess(weights: Map[Int, Double], features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (weights.contains(f)) score += weights(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new ApplySpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val textData = sc.textFile(args.input())
    val model = sc.textFile(args.model())
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val weights = model
      .map(line => {
        var split = line.replaceAll("[\\(\\)]", "").split(",")
        // (feature, trainedweight)
        (split(0).toInt, split(1).toDouble)
      })
    val modelWeights = sc.broadcast(weights.collectAsMap())

    val predictions = textData
      .map(line => {
        var data = line.split(" ")
        var docID = data(0)
        var label = data(1)
        var features = data.drop(2).map(f => f.toInt)

        val score = spamminess(modelWeights.value, features)
        // spam threshold is 0
        val prediction = if(score > 0) "spam" else "ham"

        (docID, label, score, prediction)
      })
      .saveAsTextFile(args.output())
  }
}