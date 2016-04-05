package edu.usc.irds.autoext.spark

import java.io.ByteArrayInputStream
import java.lang

import edu.usc.irds.autoext.base.SimilarityComputer
import edu.usc.irds.autoext.spark.Utils._
import edu.usc.irds.autoext.tree._
import edu.usc.irds.autoext.utils.Timer
import edu.usc.irds.lang.Function
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.apache.nutch.protocol.Content
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.cyberneko.html.parsers.DOMParser
import org.kohsuke.args4j.Option
import org.xml.sax.InputSource

import scala.collection.mutable.ArrayBuffer

/**
  * This tool Computes Similarity between documents
  */
class ContentSimilarityComputer extends IOSparkJob {

  //TODO: make it easy to experiment with structure and style combination
  @Option(name = "-structWeight")
  var structSimWeight: Double = 0.0

  var writeEntries = true
  var writeMatrix = true

  var similarityComputer: SimilarityComputer[TreeNode] = null
  val htmlFilter:Function[String, lang.Boolean] = new ContentFilter("ml")

  def run(): Unit = {
    LOG.info("Starting Spark Context for similarity Computer.")

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

    val paths = getInputPaths()
    val rdds = paths.map(sc.sequenceFile(_, classOf[Text], classOf[Content]))
    val rdd = sc.union(rdds)
    var (idRdd, entryRDD) = computeSimilarity(rdd)
    entryRDD = entryRDD.cache()
    idRdd.map({case(idx, url) => s"$idx,$url"}).saveAsTextFile(outPath + "-ids")
    if (writeEntries) {
      LOG.info(s"Storing Entries at $outPath (object file)")
      entryRDD.saveAsObjectFile(outPath)
    }
    if (writeMatrix){
      val outMatrixPath = outPath + "-matrix"
      LOG.info(s"Storing Matrix at $outMatrixPath")
      new CoordinateMatrix(entryRDD)
        .toIndexedRowMatrix.rows
        .coalesce(1)
        .saveAsTextFile(outMatrixPath)
    }
  }

  /**
    * Computes similarity of documents in given sequence file
    * @param input Content RDD
    */
  private def  computeSimilarity(input: RDD[(Text, Content)])
  : (RDD[(Long, String)], RDD[MatrixEntry]) ={
    val rdd = input.filter(t => t._2.getContentType.contains("ml") || t._2.getContentType.contains("text"))//get only text or html
      .map(t => (new Text(t._1), cloneContent(t._2)))

    var treeRDD: RDD[(Text, TreeNode)] = rdd.map(tuple => {
      val content:Content = tuple._2

      LOG.info("Parsing: {}", content.getUrl)
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
        //val st = System.currentTimeMillis()
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
        //println(f"$i%d x $j%d : ${System.currentTimeMillis() - st}%dms")
        entries.toTraversable
      })
    (idRdd, entryRDD)
  }
}

object ContentSimilarityComputer {

  def main(args: Array[String]) {
    val timer = new Timer
    new ContentSimilarityComputer().run(args)
    println("Time Taken : " + timer.read())
  }
}
