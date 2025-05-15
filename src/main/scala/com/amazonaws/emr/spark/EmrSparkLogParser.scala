package com.amazonaws.emr.spark

import com.amazonaws.emr.spark.analyzer.AppAnalyzer
import com.amazonaws.emr.spark.models.AppContext
import com.github.luben.zstd.ZstdInputStream
import com.ning.compress.lzf.LZFInputStream
import net.jpountz.lz4.LZ4BlockInputStream
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkConf
import org.apache.spark.utils.SparkHadoopHelper
import org.json4s.{DefaultFormats, _}
import org.json4s.native.JsonMethods._
import org.xerial.snappy.SnappyInputStream

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import java.nio.file.Files

class EmrSparkLogParser(eventLogPath: String) extends Logging {

  private val listener = new EmrSparkListener()

  private val sparkConf = new SparkConf()

  private val hadoopConf = SparkHadoopHelper.newConfiguration(sparkConf)

  private val fs = FileSystem.get(new URI(eventLogPath), hadoopConf)

  val path = new Path(eventLogPath)

  def analyze(appContext: AppContext, options: Map[String, String]): AppContext = {
    AppAnalyzer.start(appContext, options)
    appContext
  }

  def process(): AppContext = {

    if (fs.getFileStatus(path).isDirectory) {
      logger.info(s"Processing directory ${path.getName}")
      val files = fs.listStatus(path)
        .filter(_.isFile)
        .filter(!_.getPath.getName.contains("appstatus_"))

      sortFilePaths(files).foreach { f =>
        logger.info(s"Processing file ${f.getPath.getName}")
        if (isLocal && isNotCompressed(f.getPath)) replyLocalFileInParallel(f.getPath.toUri.getRawPath)
        else replayFile(f.getPath)
      }
    } else {
      logger.info(s"Processing file ${path.getName}")
      if (isLocal && isNotCompressed(path)) replyLocalFileInParallel(eventLogPath)
      else replayFile(path)
    }
    listener.finalUpdate()
  }

  private def sortFilePaths(filePaths: Seq[FileStatus]): Seq[FileStatus] = {
    val EVENT_LOG_V2_FILE_PATTERN = """^events_(\d+)""".r

    if (EVENT_LOG_V2_FILE_PATTERN.findFirstMatchIn(filePaths.head.getPath.getName).isDefined) {
      filePaths.sortWith { case (a, b) =>
        val fileIndexA = EVENT_LOG_V2_FILE_PATTERN.findFirstMatchIn(a.getPath.getName).map(_.group(1)).get.toInt
        val fileIndexB = EVENT_LOG_V2_FILE_PATTERN.findFirstMatchIn(b.getPath.getName).map(_.group(1)).get.toInt
        fileIndexA < fileIndexB
      }
    }
    else {
      filePaths.sortBy(_.getModificationTime)
    }

  }

  private def replayFile(path: Path): Unit = {
    val replayBusClass = Class.forName("org.apache.spark.scheduler.ReplayListenerBus")
    val replayBus = replayBusClass.getDeclaredConstructor().newInstance()
    val replayMethod = replayBusClass.getMethod(
      "replay",
      classOf[InputStream],
      classOf[String],
      classOf[Boolean],
      classOf[String => Boolean])
    val addListenerMethod = replayBusClass.getMethod("addListener", classOf[Object])
    addListenerMethod.invoke(replayBus, listener)

    try {
      replayMethod.invoke(replayBus, getDecodedInputStream(path), path.getName, boolean2Boolean(false), getFilter _)
    } catch {
      case x: Exception => logger.error(s"Failed replaying events from ${path.getName} [${x.getMessage}]", x)
    }
  }

  private def replyLocalFileInParallel(absolutePath: String): Unit = {
    val replayBusClass = Class.forName("org.apache.spark.utils.CustomReplayListenerBus")
    val replayBus = replayBusClass.getDeclaredConstructor().newInstance()
    val replayMethod = replayBusClass.getMethod("replay", classOf[String], classOf[String => Boolean])

    val listenerMethod = replayBusClass.getMethod("addListener", classOf[Object])
    listenerMethod.invoke(replayBus, listener)

    Files.lines(java.nio.file.Paths.get(absolutePath))
      .parallel()
      .forEach(line => replayMethod.invoke(replayBus, line, getFilter _))
  }

  private def isLocal = path.getFileSystem(hadoopConf).getScheme.equals("file")

  private def isCompressed(path: Path): Boolean = {
    val logName = path.getName.stripSuffix(".inprogress")
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    codecName.getOrElse("") match {
      case "lz4" => true
      case "lzf" => true
      case "snappy" => true
      case "zstd" => true
      case _ => false
    }
  }

  private def isNotCompressed(path: Path): Boolean = !isCompressed(path)

  private def getDecodedInputStream(path: Path): InputStream = {
    val bufStream = new BufferedInputStream(fs.open(path))
    val logName = path.getName.stripSuffix(".inprogress")
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    codecName.getOrElse("") match {
      case "lz4" => new LZ4BlockInputStream(bufStream)
      case "lzf" => new LZFInputStream(bufStream)
      case "snappy" => new SnappyInputStream(bufStream)
      case "zstd" => new ZstdInputStream(bufStream)
      case _ => bufStream
    }
  }

  private def getFilter(eventString: String): Boolean = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val extracted = parse(eventString).extract[Map[String, Any]]
    eventFilter.contains(extracted("Event").asInstanceOf[String])
  }

  private def eventFilter: Set[String] = {
    Set(
      "SparkListenerLogStart",
      "SparkListenerApplicationStart",
      "SparkListenerApplicationEnd",
      "SparkListenerBlockManagerAdded",
      "SparkListenerBlockManagerRemoved",
      "SparkListenerEnvironmentUpdate",
      "SparkListenerExecutorAdded",
      "SparkListenerExecutorRemoved",
      "SparkListenerJobStart",
      "SparkListenerJobEnd",
      "SparkListenerStageSubmitted",
      "SparkListenerStageCompleted",
      "SparkListenerTaskEnd",
    )
  }

}
