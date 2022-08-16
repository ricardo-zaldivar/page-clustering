/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.usc.irds.autoext.spark

import java.io.ByteArrayInputStream
import java.lang
import edu.usc.irds.autoext.base.SimilarityComputer
import edu.usc.irds.autoext.spark.ContentSimilarityComputer._
import edu.usc.irds.autoext.tree._
import edu.usc.irds.autoext.utils.Timer
import edu.usc.irds.lang.Function
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs._
import org.apache.hadoop.io.Text
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.cyberneko.html.parsers.DOMParser
import org.kohsuke.args4j.Option
import org.xml.sax.InputSource

import java.net.URI

/**
  * This tool Computes Similarity between documents
  */
class ContentSimilarityComputer extends SparkJob {

  @Option(name = "-func", required = true,
    usage = "Similarity function. Valid function names = {structure, style}")
  var simFunc: String = null

  var simComputer: SimilarityComputer[TreeNode] = null
  val htmlFilter: Function[String, lang.Boolean] = new ContentFilter("ml")

  def run(): Unit = {

    simComputer = simFunc match {
      case STRUCTURE => new StructureSimComputer()
      case STYLE => new StyleSimComputer()
      case _ => throw new IllegalArgumentException(s"Similarity function $simFunc is not supported")
    }

    val fileSystem = FileSystem.get(URI.create(s3Path), sc.hadoopConfiguration)
    val files = fileSystem.listFiles(new Path(s3Path), true)

    var pages: List[(Text, Array[Byte])] = List()

    while (files.hasNext) {
      val nextFile = files.next()
      if (nextFile.isDirectory || nextFile.getPath.getName.startsWith(".")){
        //that's fine, skip it
      } else if (nextFile.isFile && nextFile.getPath.getName.endsWith(".html")) {
        val id = nextFile.getPath.toString
        val allBytes: Array[Byte] = sc.textFile(nextFile.getPath.toString).flatMap(s => s.getBytes()).collect()
        val ap = new Text(id)

        pages = pages:+((ap, allBytes))
        LOG.info(id)
      } else {
        LOG.warn(s"Skip : $nextFile" )
      }
    }

    val rdd = sc.parallelize(pages)
    rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val (idRdd, entryRDD) = computeSimilarity(rdd)

    val outPath = s"${s3Path}results/$simFunc"
    LOG.info(s"Storing Ids to URL map at $outPath (CSV File)")
    idRdd.map({case(idx, url) => s"$idx,$url"}).coalesce(1).saveAsTextFile(outPath + "-ids")

    LOG.info(s"Storing Entries at $outPath (object file)")
    entryRDD.saveAsObjectFile(outPath)
  }

  /**
    * Computes similarity of documents in given sequence file
    * @param input Content RDD
    */
  private def  computeSimilarity(input: RDD[(Text, Array[Byte])])
  : (RDD[(Long, String)], RDD[MatrixEntry]) ={
    // local variable serialization, otherwise we need to serialize 'this' whole object
    val LOG = this.LOG
    val computer = simComputer

    val rdd = input.filter(t => t._1.toString.contains("html"))//get only text or html
      .map(t => (new Text(t._1), t._2))

    var treeRDD: RDD[(Text, TreeNode)] = rdd.map({case (key, content) =>
      var stream: ByteArrayInputStream = null
      var res: (Text, TreeNode) = null
      try {
        stream = new ByteArrayInputStream(content)
        val parser = new DOMParser()
        parser.parse(new InputSource(stream))
        val doc = parser.getDocument
        val elements = doc.getElementsByTagName("HTML")
        if (elements.getLength > 0) {
          val tree = TreeNode.create(elements.item(0), key.toString)
          res = (key, tree)
        }
      } catch {
        case  e: Exception =>
          LOG.error(e.getMessage)
          res = null //error case
      } finally {
        IOUtils.closeQuietly(stream)
      }
      res
    }).filter(_ != null)

    treeRDD = treeRDD.persist() //cache here so that spark dont end up re-parsing again and again

    val iRdd: RDD[(Long, TreeNode)] = treeRDD
      .zipWithIndex()
      .map({case ((k, tree), idx) => (idx, tree)})

    val idRdd = iRdd.map({case (id, tree) => (id, tree.getExternalId)})
    var pairs = iRdd.cartesian(iRdd)

    // throw away lower diagonal
    pairs = pairs.filter({case ((i, t1), (j, t2)) => i >= j}).cache()
    LOG.info("Num Partitions: {}",  pairs.partitions.length)

    val entryRDD: RDD[MatrixEntry] = pairs.flatMap({ case ((i, treeI), (j, treeJ)) =>
        val res =
        if (i == j) {
          //principal diagonal => same  tree
          Array(new MatrixEntry(i, j, 1.0))
        } else {
          val score = computer.compute(treeI, treeJ)
          Array(new MatrixEntry(i, j, score), new MatrixEntry(j, i, score)) //symmetry
        }
        //println(f"$i%d x $j%d : ${System.currentTimeMillis() - st}%dms")
        res.toTraversable
      })
    //return ids as well as entries
    (idRdd, entryRDD)
  }
}

object ContentSimilarityComputer {

  val STRUCTURE = "structure"
  val STYLE = "style"

  def main(args: Array[String]) {
    val timer = new Timer
    new ContentSimilarityComputer().run(args)
    println("Time Taken : " + timer.read())
  }
}
