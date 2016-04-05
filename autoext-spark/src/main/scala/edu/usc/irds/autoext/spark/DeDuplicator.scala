package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content

/**
  * A de-duplicator tool
  */
class DeDuplicator extends IOSparkJob{

  def run(): Unit = {

    val rdd = sc.union(getInputPaths()
      .map(sc.sequenceFile(_, classOf[Text], classOf[Content]))) // club all parts

    rdd.map({case (k,v) => (new Text(k), Utils.cloneContent(v))})
      .groupByKey()
      .map({case (k, v) => (k, v.iterator.next())})
      .saveAsHadoopFile(outPath, classOf[Text],
        classOf[Content], classOf[SequenceFileOutputFormat[Text,Content]]) // save it

    LOG.info(s"Done. Saved output at $outPath")
  }
}

object DeDuplicator extends {

  def main(args: Array[String]) {
    new DeDuplicator().run(args)
  }
}
