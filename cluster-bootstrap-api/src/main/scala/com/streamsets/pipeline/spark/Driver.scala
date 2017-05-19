/**
  * Copyright 2017 StreamSets Inc.
  *
  * Licensed under the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Driver {

  val transformers: ArrayBuffer[SparkTransformer] = ArrayBuffer()
  private var previousIncomingData: Option[RDD[(Array[Byte], Array[Byte])]] = Option.empty
  private var previousGeneratedRDDs: mutable.Queue[RDD[Record]] = mutable.Queue()
  private val SDC_MESOS_BASE_DIR = "sdc_mesos"
  private var initialized = false
  private var isPreprocessingMesosDone = false
  private val EMPTY_LIST = new util.ArrayList[Record]()
  private val ERROR_HEADER = "streamsetsInternalClusterErrorReason"
  private var offsetManager: KafkaOffsetManager = _

  def foreach(dstream: DStream[(Array[Byte], Array[Byte])], kafkaOffsetManager: KafkaOffsetManager) {
    dstream.foreachRDD(rdd => process(rdd))
    offsetManager = kafkaOffsetManager
  }

  def process(rdd: RDD[(Array[Byte], Array[Byte])]): Unit = synchronized {
    previousIncomingData.foreach(_.unpersist(false))

    previousGeneratedRDDs.clear()
    previousGeneratedRDDs.foreach(_.unpersist(false))

    previousIncomingData = Option(rdd)
    val count = rdd.count()
    if (count == 0) {
      return
    }

    if (transformers.isEmpty) {
      transformers ++= BootstrapCluster.getTransformers.asScala
    }

    val partitionCount =  rdd.partitions.length

    var nextResult: RDD[Record] =  rdd.mapPartitions(iterator => {
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

    val c = nextResult.cache().count()
    var id = 0
    transformers.foreach(transformer => {

      val result = transformer.transform(nextResult)

      def repartition[T](rdd: RDD[T]) : RDD[T] = {
        if (rdd.partitions.length != partitionCount) rdd.repartition(partitionCount)
        else rdd
      }

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
    offsetManager.saveOffsets(rdd)
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
