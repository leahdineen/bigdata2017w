package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.MutableList
import scala.collection.mutable.Map
import scala.math

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  verify()
}
object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())


  // Scores a document based on its list of features.
  def spamminess(w: Map[Int, Double], features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new TrainSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val trainingData = sc.textFile(args.input())
    val modelDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(modelDir, true)

    val trained = trainingData
      .map(line => {
        var data = line.split(" ")
        var docID = data(0)
        var label = data(1)
        var features = data.drop(2).map(f => f.toInt)

        // key 0 so all training points are sent to the same reducer
        (0, (docID, label, features))
      })
      .groupByKey(1)
      .flatMap(x => {
        // learned weights
        val w = Map[Int, Double]()

        val delta = 0.002

        // For each training point update weights
        x._2.foreach(v => {
          val isSpam = if(v._2 == "spam") 1 else 0
          val features = v._3

          // Update the weights as follows:
          val score = spamminess(w, features)
          val prob = 1.0 / (1 + math.exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        })

        val trained_weights = MutableList[(Int, Double)]()
        // emit values of map as tuple
        w.foreach(y => {
          trained_weights += y
        })
        trained_weights
      })
      .saveAsTextFile(args.model())
  }
}