/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import com.streamsets.pipeline.lib.xml.StreamingXmlParser;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class SDCRecordCreator implements RecordCreator {
  private final Source.Context context;

  public SDCRecordCreator(Source.Context context) {
    this.context = context;
  }

  @Override
  public List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException {
    try (Reader reader = new InputStreamReader(new ByteArrayInputStream(message.getPayload()))) {
      try (JsonRecordReader recordReader = ((ContextExtensions) context).createJsonRecordReader(reader, 0,
                                                                                                Integer.MAX_VALUE)) {
        List<Record> list = new ArrayList<>();
        Record record = recordReader.readRecord();
        while (record != null) {
          list.add(record);
          record = recordReader.readRecord();
        }
        return list;
      } catch (IOException ex) {
        throw new StageException(KafkaStageLibError.KFK_0101, ex.getMessage(), ex);
      }
    } catch (IOException ex) {
      throw new StageException(KafkaStageLibError.KFK_0101, ex.getMessage(), ex);

    }
  }

}
