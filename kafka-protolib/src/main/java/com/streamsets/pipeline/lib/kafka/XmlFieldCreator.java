/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import com.streamsets.pipeline.lib.xml.StreamingXmlParser;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

public class XmlFieldCreator implements FieldCreator {

  @Override
  public Field createField(byte[] bytes) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes))))) {
      StreamingXmlParser xmlParser = new StreamingXmlParser(reader);
      return xmlParser.read();
    } catch (Exception e) {
      throw new StageException(KafkaStageLibError.KFK_0101, e.getMessage(), e);
    }
  }
}
