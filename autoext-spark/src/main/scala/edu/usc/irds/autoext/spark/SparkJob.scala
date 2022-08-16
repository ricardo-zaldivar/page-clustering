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

import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.{CmdLineParser, Option}
import org.slf4j.LoggerFactory

/**
  * Base class for all spark jobs
  */
trait SparkJob extends CliTool {

  val LOG = LoggerFactory.getLogger(getClass)

  @Option(name = "-master", aliases = Array("--master"),
  usage = "Spark master. This is not required when job is started with spark-submit")
  var sparkMaster: String = null

  @Option(name = "-app", aliases= Array("--app-name"),
  usage = "Name for spark context.")
  var appName: String = getClass.getSimpleName

  @Option(name = "-s3endpoint", aliases= Array("--s3-endpoint"),
    usage = "S3 Bucket endpoint")
  var s3Endpoint: String = null

  @Option(name = "-s3AccessKey", aliases= Array("--s3-access-key"),
    usage = "S3 Bucket access key")
  var s3AccessKey: String = null

  @Option(name = "-s3SecretKey", aliases= Array("--s3-secret-key"),
    usage = "S3 Bucket secret key")
  var s3SecretKey: String = null

  @Option(name = "-s3BucketName", aliases= Array("--s3-bucket-name"),
    usage = "S3 Bucket name")
  var s3BucketName: String = null

  @Option(name = "-s3Folder", aliases= Array("--s3-folder"),
    usage = "S3 Bucket folder name")
  var s3Folder: String = null

  var s3Path: String = null

  var sc: SparkContext = null

  override def parseArgs(args:Array[String]): Unit ={
    super.parseArgs(args)
    if (s3BucketName == null && s3Folder == null) {
      System.err.println("Either -s3BucketName or -s3Folder is required.")
      new CmdLineParser(this).printUsage(System.err)
      System.exit(1)
    }
  }

  /**
    * initializes spark context if not already initialized
    */
  def initSpark(): Unit ={
    if (sc == null) {
      LOG.info("Initializing Spark Context ")
      val conf = new SparkConf().setAppName(appName)
        .registerKryoClasses(Array(classOf[Text], classOf[Array[Byte]]))
      if (sparkMaster != null) {
        LOG.info("Spark Master {}", sparkMaster)
        conf.setMaster(sparkMaster)
      }
      sc = new SparkContext(conf)
      sc.hadoopConfiguration.set("fs.s3a.endpoint", s3Endpoint)
      sc.hadoopConfiguration.set("fs.s3a.access.key", s3AccessKey)
      sc.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
    }

    s3Path = s"s3a://$s3BucketName/$s3Folder/"
  }

  def stopSpark(): Unit ={
    if (sc != null){
      LOG.info("Stopping spark.")
      sc.stop()
    }
  }

  /**
    * Abstract method which has actual job description
    */
  def run()

  def run(args:Array[String]): Unit ={
    parseArgs(args)
    initSpark()
    run()
    stopSpark()
  }
}
