package edu.usc.irds.autoext.spark

import java.io.{ByteArrayInputStream, IOException}
import java.lang
import java.net.URL
import java.util.function.Function

import edu.usc.irds.autoext.base.SimilarityComputer
import edu.usc.irds.autoext.io.RDDMultipleOutputFormat
import edu.usc.irds.autoext.spark.ContentCluster._
import edu.usc.irds.autoext.tree._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.cyberneko.html.parsers.DOMParser
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import org.slf4j.{Logger, LoggerFactory}
import org.xml.sax.InputSource

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source



/**
  * Content Cluster APP using Spark Scala API
  */
class ContentCluster {

  @Option(name = "-list", required = true,
    usage = "List of Nutch Segment(s) Part(s)")
  var pathListFile: String = null

  @Option(name = "-workdir", required = true, usage = "Work directory.")
  var workDirName: String = null

  @Option(name = "-master", usage = "Spark master url")
  var masterUrl: String = "local[2]"

  @Option(name = "-sw", aliases = Array("--sim-weight"),
    usage = "weight used for aggregating structural and style similarity measures.\n" +
      "Range : [0.0, 1.0] inclusive\n" +
      "Notes :\n\t0.0 disables structural similarity and only style similarity will be used (it is faster)\n" +
      "\t1.0 disables style similarity and thus only structural similarity will be used\n")
  var structSimWeight:Double = 0.0

  var appName = "Web documents Clustering"

  var sc : SparkContext = null
  var workDir:Path = null
  var parts: List[String] = null
  var domainsDir: Path = null
  var simDir: Path = null
  var hConf: Configuration = null
  var similarityComputer: SimilarityComputer[TreeNode] = null
  val htmlFilter:Function[String, lang.Boolean] = new ContentFilter("html")

  def init(): Unit = {
    this.sc = new SparkContext(new SparkConf()
      .setMaster(masterUrl).setAppName(appName))
    this.workDir = new Path(workDirName)
    val textCleanFilter: String => Boolean = p => !p.isEmpty && !p.startsWith("#")
    this.parts = Source.fromFile(pathListFile)
      .getLines().toList.map(s => s.trim).filter(textCleanFilter)
    this.domainsDir = new Path(workDir, DOMAINS_DIR)
    this.simDir = new Path(workDir, SIMILARITY_DIR)
    this.hConf = new Configuration
    val fs = FileSystem.get(hConf)
    fs.mkdirs(workDir)
    fs.mkdirs(new Path(simDir, ENTRIES_DIR))
    fs.mkdirs(new Path(simDir, MATRIX_DIR))


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
      similarityComputer = GrossSimComputer.createWebSimilarityComputer(structSimWeight)
    }

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

  /**
    * Computes similarity of documents in given sequence file
    * @param file sequence file containing Nutch Content
    */
  private def computeSimilarity(file: FileStatus): Unit ={
    val path: String = file.getPath.toString
    LOG.info("Processing {} ", path)

    val rdd = new IndexedNutchContentRDD(sc, path, htmlFilter)
    val treeRDD: RDD[(Long, TreeNode)] = rdd.map(tuple => {
      // println("Tree :: " + tuple.getIndex)
      var stream: ByteArrayInputStream = null
      var res : (Long, TreeNode) = null
      try {
        val content = tuple.getContent
        stream = new ByteArrayInputStream(content.getContent)
        val parser = new DOMParser()
        parser.parse(new InputSource(stream))
        val doc = parser.getDocument
        val elements = doc.getElementsByTagName("HTML")
        if (elements.getLength > 0) {
          val tree = new TreeNode(elements.item(0), null)
          res = (tuple.getIndex, tree)
        }
      } catch {
        case  e: Exception => {
          LOG.error(e.getMessage)
          res = null //error case
        }
      } finally {
        IOUtils.closeQuietly(stream)
      }
      res
    }).filter(f => f != null)

    //treeRDD = treeRDD.cache

    var pairs = treeRDD.cartesian(treeRDD)
    // throw away lower diagonal
    pairs = pairs.filter(f => f._1._1 >= f._2._1)

    val computer = similarityComputer // local variable serialization, otherwise we need to serialize 'this' whole object
    var entryRDD:RDD[MatrixEntry] = pairs.flatMap(tup => {
        val i = tup._1._1
        val j = tup._2._1
        //println(f"$i%d x $j%d")
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
        //println("Time taken :" + (System.currentTimeMillis() - st))
        entries.toTraversable
      })

    entryRDD = entryRDD.cache()
    val outPathSeq: Path = new Path(simDir, s"$ENTRIES_DIR/${file.getPath.getName}")
    entryRDD.map(e => (new LongWritable(e.i), new Text(s"${e.j}:${e.value}")))
      .saveAsHadoopFile(outPathSeq.toString, classOf[LongWritable], classOf[Text],
        classOf[SequenceFileOutputFormat[_, _]])

    val outPathMatrix: Path = new Path(simDir, s"$MATRIX_DIR/${file.getPath.getName}")
    val similarityMatrix: CoordinateMatrix = new CoordinateMatrix(entryRDD)
    similarityMatrix.toIndexedRowMatrix.rows.saveAsTextFile(outPathMatrix.toString)
  }

  @throws(classOf[IOException])
  private def computeSimilarity() {
    FileSystem.get(hConf)
      .listStatus(domainsDir, new PathFilter {
        override def accept(path: Path): Boolean =
          !(path.getName.startsWith("_") || path.getName.startsWith("."))
      }).filter(f => f.isFile)
      .foreach(file => {
        //TODO: parallellize
        try {
          computeSimilarity(file)
        } catch {
          case e: Exception =>
            LOG.error("Failed : {}", file.getPath)
            LOG.error(e.getMessage, e)
        }
      })
  }

  /**
    * Cluster The documents based on shared neighbor approach
    */
  def cluster(): Unit = {
    val parentDir = new Path(workDir, s"$SIMILARITY_DIR/$ENTRIES_DIR")
    val files = FileSystem.get(hConf).listFiles(parentDir, true)
    while(files.hasNext){
      val file = files.next()
      if (file.isFile && file.getPath.getName.startsWith("part-")) {
        val path:String = file.getPath.toString
        LOG.info("Processing {}", path)
        val entries = sc.sequenceFile(path, classOf[LongWritable], classOf[Text])
          .map(tup => {
            val parts = tup._2.toString.split(":")
            new MatrixEntry(tup._1.get(), parts(0).toLong, parts(1).toDouble)
          })
        val matrix = new CoordinateMatrix(entries)
        println(s"${matrix.numCols()} x ${matrix.numRows()}")
        //FIXME: implement shared neighbor clustering
      }
    }
  }
}

/**
  * Created by tg on 1/31/16.
  */
object ContentCluster {
  val DOMAINS_DIR = "domains"
  val SIMILARITY_DIR = "similarity"
  val ENTRIES_DIR = "entries"
  val MATRIX_DIR = "matrix"
  val LOG: Logger = LoggerFactory.getLogger(classOf[ContentCluster])


  def main(args: Array[String]) {

    //var args = "-list list.txt -workdir out-2".split(" ")
    val instance = new ContentCluster
    val parser = new CmdLineParser(instance)
    try {
      parser.parseArgument(args.toList.asJava)
    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }

    LOG.info("Initializing...")
    instance.init()
    LOG.info("Initialization complete")
    LOG.info("Separating domains...")
    instance.separateDomains()
    LOG.info("Domains separation complete")
    LOG.info("Computing similarity...")
    instance.computeSimilarity()
    LOG.info("Similarity Computation done...")

    LOG.info("Clustering...")
    instance.cluster()
    LOG.info("Computing complete!")
  }
}

