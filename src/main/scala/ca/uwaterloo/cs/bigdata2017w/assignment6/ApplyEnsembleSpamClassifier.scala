package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import scala.collection.Map
import scala.math

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output, method)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "ensemble method", required = true)
  verify()
}
object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // Scores a document based on its list of features.
  def spamminess(w: Map[Int, Double], features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new ApplyEnsembleSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val textData = sc.textFile(args.input())
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // TODO: read in a better way
    // read each model into a list
    val models = MutableList(sc.textFile(args.model() + "/part-00000"), sc.textFile(args.model() + "/part-00001"), sc.textFile(args.model() + "/part-00002"))

    val weights = models
      .map(m => {
        m.map(line => {
          var split = line.replaceAll("[\\(\\)]", "").split(",")
          // (feature, trainedweight)
          (split(0).toInt, split(1).toDouble)
        })
        .collectAsMap()
      })
    val modelWeights = sc.broadcast(weights)

    // TODO: value check the method
    val method = sc.broadcast(args.method())

    val predictions = textData
      .map(line => {
        var data = line.split(" ")
        var docID = data(0)
        var label = data(1)
        var features = data.drop(2).map(f => f.toInt)

        // calculate score for each model in list
        var scores = MutableList[Double]()
        for(i <- 0 until modelWeights.value.size){
          scores += spamminess(modelWeights.value.get(i).get, features)
        }

        if (method.value == "average") {
          var score = scores.sum / scores.size.toDouble
          // spam threshold is 0
          var prediction = if(score > 0) "spam" else "ham"
          (docID, label, score, prediction)
        }
        else if (method.value == "vote") {
          var score = 0
          scores.foreach(s => {
            if(s > 0) score += 1 
            else score -= 1
          })
          // spam threshold is 0
          val prediction = if(score > 0) "spam" else "ham"
          (docID, label, score, prediction)
        }
      })
      .saveAsTextFile(args.output())
  }
}