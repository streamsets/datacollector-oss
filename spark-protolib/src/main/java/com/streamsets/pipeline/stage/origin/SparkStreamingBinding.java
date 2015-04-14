/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;
import org.apache.spark.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;


import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamingBinding {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingBinding.class);
  private String master;
  private String hostname;
  private int port;
  private SparkConf conf;
  private JavaStreamingContext ssc;
  private EmbeddedSDCConf sdcConf;

  public SparkStreamingBinding(String master, String hostname, int port, EmbeddedSDCConf sdcConf) {
    this.master = master;
    this.hostname = hostname;
    this.port = port;
    this.sdcConf = sdcConf;
  }

  public void destroy() {
    if (ssc != null) {
      ssc.stop();
    }
  }

  public void init() throws StageException {
    // TODO remove this hack from LogUtils
    System.setProperty("spark.streaming.streamsets", "true");
    LOG.info("Starting spark streaming source");
    conf = new SparkConf().setAppName("StreamSets Data Collector").setMaster(master);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    ssc = new JavaStreamingContext(conf, new Duration(1000));
    JavaDStream<String> dStream = ssc.socketTextStream(hostname, port);
    dStream.foreachRDD(new SparkDriverFunction(sdcConf));
    ssc.start();
  }
}
