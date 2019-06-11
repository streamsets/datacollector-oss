/**
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.spark

import java.io.File
import java.util

import com.streamsets.pipeline.api.Record
import com.streamsets.pipeline.spark.api.SparkTransformer
import com.streamsets.pipeline.{BootstrapCluster, ClusterFunctionProvider}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Driver {

  val transformers: ArrayBuffer[SparkTransformer] = ArrayBuffer()
  private var previousIncomingData: mutable.Queue[RDD[(Array[Byte], Array[Byte])]] = mutable.Queue()
  private var previousGeneratedRDDs: mutable.Queue[RDD[Record]] = mutable.Queue()
  private val SDC_MESOS_BASE_DIR = "sdc_mesos"
  private var initialized = false
  private var isPreprocessingMesosDone = false
  private val EMPTY_LIST = new util.ArrayList[Record]()
  private val ERROR_HEADER = "streamsetsInternalClusterErrorReason"
  private var offsetManager: KafkaOffsetManager = _
  private var partitionCount = -1

  def foreach(dstream: DStream[ConsumerRecord[Array[Byte], Array[Byte]]], kafkaOffsetManager: KafkaOffsetManager) {
    offsetManager = kafkaOffsetManager
    dstream.foreachRDD(rdd => {
      process(
        rdd.map(c => {
          (c.key(), c.value())
        })
      )
      offsetManager.saveOffsets(rdd)
    })
  }

  def foreachTuple(dstream: DStream[(Array[Byte], Array[Byte])], kafkaOffsetManager: KafkaOffsetManager) {
    offsetManager = kafkaOffsetManager
    dstream.foreachRDD(rdd => {
       process(rdd)
       offsetManager.saveOffsets(rdd)
     })
  }

  def process(rdd: RDD[(Array[Byte], Array[Byte])]): Unit = synchronized {
    previousIncomingData.foreach(_.unpersist(false))
    previousIncomingData.clear()

    previousGeneratedRDDs.foreach(_.unpersist(false))
    previousGeneratedRDDs.clear()

    previousIncomingData += rdd

    if (transformers.isEmpty) {
      transformers ++= BootstrapCluster.getTransformers.asScala
    }

    if (partitionCount == -1) {
      // Count the number of executors (subtract 1 since the getExecutorStorageStatus
      // returns the storage stages for the driver as well.
      if(rdd.sparkContext.getClass.getMethods.map(_.getName).filter(_ matches "getExecutorStorageStatus").length > 0){
        // For Spark versions < 2.4.0
        partitionCount = rdd.sparkContext.getExecutorStorageStatus.length - 1
      }else{
        // For Spark versions >= 2.4.0
        partitionCount = rdd.sparkContext.statusTracker.getExecutorInfos.length
      }
    }

    def repartition[T: ClassTag](rdd: RDD[T]) : RDD[T] = {
      if (rdd.partitions.length > partitionCount) {
        // Hack: RDD.coalesce() changed in non-binary compatible way between Spark 1.x and 2.y
        // (but code-compatible, since a new param with default was added), but the JavaRDD.coalesce() method is binary
        // compatible between Spark 1.x and 2.y, so use that to coalesce and return underlying RDD
        JavaRDD.fromRDD(rdd).coalesce(partitionCount).rdd
      } else if (rdd.partitions.length < partitionCount) {
        rdd.repartition(partitionCount)
      } else {
        rdd
      }
    }

    val incoming = if (transformers.nonEmpty) repartition(rdd) else rdd

    previousIncomingData += incoming

    var nextResult: RDD[Record] =  incoming.mapPartitions(iterator => {
      initialize()
      val batch = iterator.map({ pair  =>
          val key = if (pair._1 == null) {
            "UNKNOWN_PARTITION".getBytes
          } else {
            pair._1
          }
          new com.streamsets.pipeline.impl.Pair(key, pair._2)
              .asInstanceOf[util.Map.Entry[_, _]]
        }).toList.asJava
      ClusterFunctionProvider.getClusterFunction.startBatch(batch).asInstanceOf[java.util.Iterator[Record]].asScala
    })
    previousGeneratedRDDs += nextResult

    nextResult.cache().count()
    var id = 0
    transformers.foreach(transformer => {

      val result = transformer.transform(nextResult)

      repartition(result.getErrors.rdd).foreachPartition(batch => {
        initialize()
        val errors = batch.map { error =>
          val record = error._1
          record.getHeader.setAttribute(ERROR_HEADER, error._2)
          record
        }
        ClusterFunctionProvider.getClusterFunction.writeErrorRecords(errors.asJava, id)
      })

      nextResult = repartition(result.getResult.rdd).mapPartitions(batch => {
        initialize()
        val batchToForward = batch.asJava.asInstanceOf[java.util.Iterator[AnyRef]]
        ClusterFunctionProvider.getClusterFunction
            .forwardTransformedBatch(batchToForward, id)
            .asInstanceOf[java.util.Iterator[Record]]
            .asScala
      }).cache()
      nextResult.count()
      previousGeneratedRDDs += nextResult
      id += 1

    })
    nextResult.count()
  }

  def extractArchives(mesosHomeDir: String): Boolean = {
    BootstrapCluster.printSystemPropsEnvVariables()
    val processExitValue =
      BootstrapCluster.findAndExtractJar(new File(mesosHomeDir), new File(System.getenv("SPARK_HOME")))
    if (processExitValue == 0) {
      System.setProperty("SDC_MESOS_BASE_DIR", new File(mesosHomeDir, SDC_MESOS_BASE_DIR).getAbsolutePath)
      true
    } else {
      false
    }
  }

  def initialize(): Unit = synchronized {
    if (initialized) {
      return
    }

    val mesosHomeDir = Option(System.getenv("MESOS_DIRECTORY"))
    mesosHomeDir.foreach(homeDir => {
      synchronized {
        if (!isPreprocessingMesosDone) {
          isPreprocessingMesosDone = extractArchives(homeDir)
        }
        if (!isPreprocessingMesosDone) {
          throw new IllegalStateException(
            "Cannot extract archives in dir:" + mesosHomeDir + "/" + SDC_MESOS_BASE_DIR
                + "; check the stdout file for more detailed errors")
        }
      }
    })
    initialized = true
  }
}
