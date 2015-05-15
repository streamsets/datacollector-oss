/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

public class StandaloneKafkaSourceFactory extends KafkaSourceFactory {

  public StandaloneKafkaSourceFactory(SourceArguments args) {
    super(args);
  }

  public BaseKafkaSource create() {
    return new StandaloneKafkaSource(args);
  }
}
