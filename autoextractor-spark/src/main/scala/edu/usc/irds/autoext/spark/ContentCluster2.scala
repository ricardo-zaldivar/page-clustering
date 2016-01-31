package edu.usc.irds.autoext.spark

import java.io.{ByteArrayInputStream, IOException}
import java.lang
import java.net.URL
import java.util.function.Function

import edu.usc.irds.autoext.nutch.{ContentIterator, IndexedNutchContentRDD, NutchContentRDD}
import edu.usc.irds.autoext.spark.ContentCluster2._
import edu.usc.irds.autoext.tree.{GrossSimComputer, TreeNode}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.Text
import org.apache.nutch.protocol.Content
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.cyberneko.html.parsers.DOMParser
import org.slf4j.{LoggerFactory, Logger}
import org.xml.sax.InputSource

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Content Cluster APP using Spark Scala API
  */
class ContentCluster2 {

  var sc : SparkContext = null
  var parts: List[String] = null
  var workDir: Path = null
  var domainsDir: Path = null
  var simDir: Path = null
  var hConf: Configuration = null

  def init(): Unit = {
    this.sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName("Web Cluster"))
    val pathsFile = "list.txt"
    this.workDir = new Path("out-1")
    val textCleaner: String => Boolean = p => !p.isEmpty && !p.startsWith("#")
    this.parts = Source.fromFile(pathsFile)
      .getLines().toList.map(s => s.trim).filter(textCleaner)
    this.domainsDir = new Path(workDir, ContentCluster.DOMAINS_DIR)
    this.simDir = new Path(workDir, ContentCluster.SIMILARITY_DIR)
    this.hConf = new Configuration
  }

  /**
    * Separates records by domains in input
    */
  private def separateDomains() {
    val filter2 = new ContentFilter("html")

    new NutchContentRDD(sc, parts.asJava, filter2)
      .map(c => (new Text(new URL(c.getUrl).getHost), c))
      .saveAsHadoopFile(domainsDir.toString, classOf[Text], classOf[Content],
        classOf[RDDMultipleOutputFormat[_, _]])
  }

  @throws(classOf[IOException])
  private def computeSimilarity {
    val fs: FileSystem = FileSystem.get(hConf)
    val files = fs.listStatus(domainsDir, new PathFilter {
      override def accept(path: Path): Boolean = !(path.getName.startsWith("_") || path.getName.startsWith("."))
    }).filter(f=> f.isFile)

    LOG.info("Found {} domain files ", files.length)
    for (file <- files) {
      try {
        val path: String = file.getPath.toString
        val name: String = file.getPath.getName
        LOG.info("Starting {}", name)

        var treeRDD: RDD[(Long, TreeNode)] = new IndexedNutchContentRDD(sc, path, ContentIterator.ACCEPT_ALL_FILTER)
          .map(tuple => {
            var stream: ByteArrayInputStream = null;
            try {
              val content = tuple.getContent
              stream = new ByteArrayInputStream(content.getContent)
              val parser = new DOMParser()
              parser.parse(new InputSource(stream))
              val doc = parser.getDocument
              stream.close()
              val tree = new TreeNode(doc.getDocumentElement, null)
              tree.setExternalId(content.getUrl)
              (tuple.getIndex, tree)
            } catch {
              case  e: Exception => LOG.error(e.getMessage)
               null //error case
            }
          }).filter(f => f != null)

        treeRDD = treeRDD.cache
        println("Total trees : " + treeRDD.count())


        var pairs = treeRDD.cartesian(treeRDD)
        pairs = pairs.filter(f => f._1._1 >= f._2._1) // throw away lower diagonal

        val entryRDD:RDD[MatrixEntry] = pairs.flatMap(tup => {
          val i = tup._1._1
          val j = tup._2._1
          var entries:ArrayBuffer[MatrixEntry] = new ArrayBuffer[MatrixEntry]()
          if (i == j) {
            //principal diagonal => same tree
            entries += new MatrixEntry(i, j, 1.0)
          } else {
            val treeI: TreeNode = tup._1._2
            val treeJ: TreeNode = tup._2._2
            val computer: GrossSimComputer[TreeNode] = GrossSimComputer.createWebSimilarityComputer(0.75);
            val score = computer.compute(treeI, treeJ)
            entries += new MatrixEntry(i, j, score)
            //symmetry
            entries += new MatrixEntry(j, i, score)
          }
          entries.toTraversable
        })

        val similarityMatrix: CoordinateMatrix = new CoordinateMatrix(entryRDD)
        val outPath: Path = new Path(simDir, name)
        similarityMatrix.toIndexedRowMatrix.rows.saveAsTextFile(outPath.toString)
      }
      catch {
        case e: Exception => {
          LOG.error("Failed : {}", file)
          LOG.error(e.getMessage, e)
        }
      }
    }
  }
}

@SerialVersionUID(100L)
private class ContentFilter(subString:String)
  extends Function[String, lang.Boolean]
  with scala.Serializable {

  override def apply(t: String): lang.Boolean = t.contains(subString)
}


/**
  * Created by tg on 1/31/16.
  */
object ContentCluster2 {
  val DOMAINS_DIR = "domains"
  val SIMILARITY_DIR = "similarity"
  val LOG: Logger = LoggerFactory.getLogger(classOf[ContentCluster])

  def main(args: Array[String]) {
    val i = new ContentCluster2
    LOG.info("Initializing...")
    i.init()
    LOG.info("Initialization complete")
    LOG.info("Separating domains...")
    //i.separateDomains()
    LOG.info("Domains separation complete")
    LOG.info("Computing similarity...")
    i.computeSimilarity
    LOG.info("Similarity Computation done...")
  }
}

