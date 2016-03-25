package edu.usc.irds.autoext.spark

import java.io.ByteArrayInputStream
import java.lang

import edu.usc.irds.autoext.base.SimilarityComputer
import edu.usc.irds.autoext.tree._
import edu.usc.irds.autoext.utils.Timer
import edu.usc.irds.lang.Function
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.cyberneko.html.parsers.DOMParser
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory
import ContentSimilarityComputer._
import org.xml.sax.InputSource

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tg on 3/18/16.
  */
class ContentSimilarityComputer extends CliTool {

  @Option(name = "-in", required = true, usage = "Path to Input Sequence File")
  var inputFile: String = null

  @Option(name = "-out", required = true, usage = "Path to output file")
  var output: String = null

  @Option(name = "-structWeight")
  var structSimWeight: Double = 0.0

  var writeEntries = true
  var writeMatrix = true

  var similarityComputer: SimilarityComputer[TreeNode] = null
  val htmlFilter:Function[String, lang.Boolean] = new ContentFilter("ml")

  def run(): Unit = {
    LOG.info("Starting Spark Context for similarity Computer.")
    val conf = new SparkConf()
      .setAppName(classOf[ContentSimilarityComputer].getName)
      .registerKryoClasses(Array(classOf[Text], classOf[Content]))
    val sc = new SparkContext(conf)

    // validate
    if (structSimWeight < 0.0 || structSimWeight > 1.0) {
      throw new IllegalArgumentException(s"The similarity weight" +
        s" $structSimWeight should be [0.0, 1.0]")
    }
    // decide similarity function
    if (Math.abs(structSimWeight - 0.0) < 0.000001) {
      // only style similarity
      similarityComputer = new StyleSimComputer()
    } else if (Math.abs(structSimWeight - 1.0) < 0.00001){
      // only structure similarity
      similarityComputer = new StructureSimComputer(new DefaultEditCost)
    } else {
      similarityComputer = GrossSimComputer.createWebSimilarityComputer()
    }
    var (idRdd, entryRDD) = computeSimilarity(sc, inputFile)
    entryRDD = entryRDD.cache()
    idRdd.saveAsTextFile(output+"-ids")
    if (writeEntries) {
      LOG.info(s"Storing Entries at $output (object file)")
      entryRDD.saveAsObjectFile(output)
    }
    if (writeMatrix){
      val outMatrixPath = output + "-matrix"
      LOG.info(s"Storing Matrix at $outMatrixPath")
      new CoordinateMatrix(entryRDD)
        .toIndexedRowMatrix.rows
        .coalesce(1)
        .saveAsTextFile(outMatrixPath)
    }
  }

  /**
    * Computes similarity of documents in given sequence file
    * @param path Path to sequence file containing Nutch Content
    */
  private def  computeSimilarity(sc: SparkContext, path: String)
  : (RDD[(Long, String)], RDD[MatrixEntry]) ={
    LOG.info("Processing {} ", path)
    val rdd = sc.sequenceFile(path, classOf[Text], classOf[Content])
      .filter(t => t._2.getContentType.contains("ml") || t._2.getContentType.contains("text"))//get only text or html
      .map(t => (new Text(t._1), cloneContent(t._2)))
    //val iRdd:RDD[(Long, Content)] = rdd.zipWithIndex().map(rec => (rec._2, rec._1._2))

    var treeRDD: RDD[(Text, TreeNode)] = rdd.map(tuple => {
      val content:Content = tuple._2

      println("Parsing: " + content.getUrl)
      var stream: ByteArrayInputStream = null
      var res : (Text, TreeNode) = null
      try {
        stream = new ByteArrayInputStream(content.getContent)
        val parser = new DOMParser()
        parser.parse(new InputSource(stream))
        val doc = parser.getDocument
        val elements = doc.getElementsByTagName("HTML")
        if (elements.getLength > 0) {
          val tree = TreeNode.create(elements.item(0), content.getUrl)
          res = (tuple._1, tree)
        }
      } catch {
        case  e: Exception =>
          LOG.error(e.getMessage)
          res = null //error case
      } finally {
        IOUtils.closeQuietly(stream)
      }
      res
    }).filter(f => f != null)

    treeRDD = treeRDD.cache() //cache here so that spark dont end up re-parsing again and again

    val iRdd: RDD[(Long, TreeNode)] = treeRDD
              .zipWithIndex()
              .map(rec => (rec._2, rec._1._2))

    val idRdd = iRdd.map(tup => (tup._1, tup._2.getExternalId))
    var pairs = iRdd.cartesian(iRdd)//.repartition(sc.defaultParallelism)
    // throw away lower diagonal
    pairs = pairs.filter(f => f._1._1 >= f._2._1).cache()
    println("Pair RDD : Num Partitions: " + pairs.partitions.length)
    val computer = similarityComputer // local variable serialization, otherwise we need to serialize 'this' whole object
    val entryRDD:RDD[MatrixEntry] = pairs.flatMap(tup => {
        val i = tup._1._1
        val j = tup._2._1
        val st = System.currentTimeMillis()
        var entries:ArrayBuffer[MatrixEntry] = new ArrayBuffer[MatrixEntry]()
        if (i == j) {
          //principal diagonal => same  tree
          entries += new MatrixEntry(i, j, 1.0)
        } else {
          val treeI: TreeNode = tup._1._2
          val treeJ: TreeNode = tup._2._2

          val score = computer.compute(treeI, treeJ)
          entries += new MatrixEntry(i, j, score)
          //symmetry
          entries += new MatrixEntry(j, i, score)
        }
        println(f"$i%d x $j%d : ${System.currentTimeMillis() - st}%dms")
        entries.toTraversable
      })
    (idRdd, entryRDD)
  }
}

object ContentSimilarityComputer {

  val LOG = LoggerFactory.getLogger(ContentSimilarityComputer.getClass)

  def cloneContent(in:Content) : Content = {
    new Content(in.getUrl, in.getBaseUrl, in.getContent,
      in.getContentType, in.getMetadata, new Configuration())
  }

  def main(args: Array[String]) {
    val timer = new Timer
    val simComp = new ContentSimilarityComputer
    simComp.parseArgs(args)
    simComp.writeMatrix = false
    simComp.structSimWeight = 0.0
    simComp.writeEntries = true
    simComp.run()
    print("Time Taken : " + timer.read())
  }
}
