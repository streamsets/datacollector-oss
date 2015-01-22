/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.codahale.metrics.Counter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.util.StageLibError;
import com.streamsets.pipeline.lib.xml.OverrunStreamingXmlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class XmlDataProducer implements DataProducer {
  private final static Logger LOG = LoggerFactory.getLogger(XmlDataProducer.class);

  private final Source.Context context;
  private final String recordElement;
  private final int maxXmlObjectLen;
  private final Counter xmlObjectsOverMaxLen;

  public XmlDataProducer(Source.Context context, String recordElement, int maxXmlObjectLen) {
    this.context = context;
    this.recordElement = recordElement;
    this.maxXmlObjectLen = maxXmlObjectLen;
    xmlObjectsOverMaxLen = context.createCounter("xmlObjectsOverMaxLen");
  }

  @Override
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException {
    String sourceFile = file.getName();
    OverrunStreamingXmlParser parser = null;
    try (Reader reader = new FileReader(file)) {
      parser = new OverrunStreamingXmlParser(reader, recordElement, offset, maxXmlObjectLen);
      return produce(sourceFile, offset, parser, maxBatchSize, batchMaker);
    } catch (XMLStreamException ex) {
      throw new BadSpoolFileException(file.getAbsolutePath(), ex.getLocation().getCharacterOffset(), ex);
    } catch (OverrunException ex) {
      throw new BadSpoolFileException(file.getAbsolutePath(), ex.getStreamOffset(), ex);
    } catch (IOException ex) {
      try {
        long exOffset = (parser != null) ? parser.getReaderPosition() : -1;
        throw new BadSpoolFileException(file.getAbsolutePath(), exOffset, ex);
      } catch (XMLStreamException ex1) {
        throw new BadSpoolFileException(file.getAbsolutePath(), -1, ex);
      }
    }
  }

  protected long produce(String sourceFile, long offset, OverrunStreamingXmlParser parser, int maxBatchSize,
      BatchMaker batchMaker) throws IOException, XMLStreamException {
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Field field = parser.read();
        if (field != null) {
          Record record = createRecord(sourceFile, offset, field);
          batchMaker.addRecord(record);
          offset = parser.getReaderPosition();
        } else {
          offset = -1;
          break;
        }
      } catch (OverrunStreamingXmlParser.XmlObjectLengthException ex) {
        xmlObjectsOverMaxLen.inc();
        context.reportError(StageLibError.LIB_0200, maxXmlObjectLen, sourceFile, ex.getXmlOffset());
        LOG.warn(StageLibError.LIB_0200.getMessage(), maxXmlObjectLen, sourceFile, ex.getXmlOffset());
      }
    }
    return offset;
  }

  protected Record createRecord(String sourceFile, long offset, Field field) throws IOException {
    Record record = context.createRecord(Utils.format("file={} offset={}", sourceFile, offset));
    record.set(field);
    return record;
  }

}
