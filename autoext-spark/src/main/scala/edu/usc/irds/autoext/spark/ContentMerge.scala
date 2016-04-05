package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

/**
  * Merges sequence parts into one sequence file with configurable number of parts
  */
class ContentMerge extends IOSparkJob {

  @Option(name = "-numparts", usage = "Number of parts in the output. Ex: 1, 2, 3.... Optional => default")
  var partitions:Integer = null

  def run(): Unit = {

    val paths = getInputPaths()
    LOG.info(s"Found ${paths.length} input paths")
    val rdds = new Array[RDD[(Text, Content)]](paths.length)
    for( i <- paths.indices){
      rdds(i) = sc.sequenceFile(paths(i), classOf[Text], classOf[Content])
    }
    var rdd = sc.union(rdds) // club all parts
    if (partitions != null) {
      rdd = rdd.coalesce(partitions)
    }
    rdd.saveAsHadoopFile(outPath, classOf[Text], classOf[Content],
      classOf[SequenceFileOutputFormat[_,_]]) // save it
    LOG.info(s"Done. Saved output at $outPath")
  }
}

object ContentMerge {
  val LOG = LoggerFactory.getLogger(DeDuplicator.getClass)
  def main(args: Array[String]) {
    new ContentMerge().run(args)
  }
}
