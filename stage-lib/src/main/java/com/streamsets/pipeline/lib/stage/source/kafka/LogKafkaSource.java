/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "text/plain")
@StageDef(version="0.0.1",
  label="Log Kafka Source")
public class LogKafkaSource extends AbstractKafkaSource {

  @Override
  protected void populateRecordFromBytes(Record record, byte[] bytes) {
    record.set(Field.create(new String(bytes)));
  }

}
