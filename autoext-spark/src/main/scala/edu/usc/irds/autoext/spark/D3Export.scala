package edu.usc.irds.autoext.spark

import java.util

import edu.usc.irds.autoext.utils.D3JsFormat
import org.apache.spark.{SparkContext, SparkConf}
import org.kohsuke.args4j.Option
import scala.collection.JavaConverters._

/**
  * This CLI Tool exports clusters into most common format used by d3js charts.
  *
  */
object D3Export extends CliTool{

  @Option(name="-in", usage = "Path to directory/file having clusters data", required = true)
  var inputFile:String = null

  @Option(name="-ids", usage = "Path to directory/file having index to id mapping. Optional.")
  var idsFile:String = null

  @Option(name="-out", usage = "Path to output file to store JSON ", required = true)
  var d3File: String = null

  @Option(name="-master", usage = "Spark master Url. Don't set this when the job is submitted " +
    "from spark-submit. Example: local[*]")
  var sparkMaster: String = null

  def main(args: Array[String]) {
    parseArgs(args)
    val conf = new SparkConf().setAppName("D3 Export -" + inputFile.split("/").last)
    if (sparkMaster != null) {
      conf.setMaster(sparkMaster)
    }

    val sc = new SparkContext(conf)

    val clusters = sc.textFile(inputFile).map(line => {
        val items = line.split(",").map(_.trim.toInt)
      (items(0), items.slice(2,  2+ items(1)).toSeq.asJava)
      }).collectAsMap().asJava.asInstanceOf[util.Map[Integer, util.List[Integer]]]

    var idsMap: util.Map[Integer, String] = null
    if (idsFile != null){
      idsMap = sc.textFile(idsFile)
        .map(line => {
          val parts = line.split(",")
          (parts(0).trim.toInt, parts(1).trim)})
        .collectAsMap().asJava.asInstanceOf[util.Map[Integer, String]]
    }

    println("Num Clusters : " + clusters.size())

    D3JsFormat.storeClusters(d3File, "Clusters 1", clusters, idsMap, 10.0f)
    println("All done")
    sc.stop()
  }
}
