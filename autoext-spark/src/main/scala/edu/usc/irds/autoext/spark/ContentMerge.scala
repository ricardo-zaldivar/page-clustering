package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory
import ContentMerge.LOG

/**
  * Merges sequence parts into one sequence file with configurable number of parts
  */
class ContentMerge extends CliTool{

  @Option(name = "-list", required = true, usage = "Path to a file having list of paths")
  var listFile: String = null

  @Option(name = "-out", required = true, usage = "Path to output.")
  var output:String = null

  @Option(name = "-numparts", usage = "Number of parts in the output. Ex: 1, 2, 3.... Optional => default")
  var partitions:Integer = null

  def run(): Unit = {
    val sConf = new SparkConf()
      .setAppName(classOf[ContentMerge].getName + s"-${System.currentTimeMillis()}")
      .registerKryoClasses(Array(classOf[Text], classOf[Content]))
    LOG.info("Creating Spark Context")
    val ctx = new SparkContext(sConf)
    val paths:Array[String] = ctx.textFile(listFile)
      .map(s => s.trim)
      .filter(s => !s.isEmpty && !s.startsWith("#"))
      .collect()
    LOG.info(s"Found ${paths.length} input paths in list file")
    val rdds = new Array[RDD[(Text, Content)]](paths.length)
    for( i <- paths.indices){
      rdds(i) = ctx.sequenceFile(paths(i), classOf[Text], classOf[Content])
    }
    var rdd = ctx.union(rdds) // club all parts
    if (partitions != null) {
      rdd = rdd.coalesce(partitions)
    }
    rdd.saveAsHadoopFile(output, classOf[Text], classOf[Content], classOf[SequenceFileOutputFormat[_,_]]) // save it
    LOG.info(s"Done. Saved output at $output")
    LOG.info("Stopping the Spark context")
    ctx.stop()
  }
}

object ContentMerge {
  val LOG = LoggerFactory.getLogger(Deduplicator.getClass)
  def main(args: Array[String]) {
    val instance = new ContentMerge
    instance.parseArgs(args)
    instance.run()
  }
}
