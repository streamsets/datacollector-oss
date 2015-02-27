/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.codahale.metrics.Counter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class RecordJsonDataProducer implements DataProducer {
  private final static Logger LOG = LoggerFactory.getLogger(RecordJsonDataProducer.class);

  private final Source.Context context;
  private final int maxJsonObjectLen;
  private final Counter jsonObjectsOverMaxLen;
  private JsonRecordReader parser;

  public RecordJsonDataProducer(Source.Context context) {
    this.context = context;
    this.maxJsonObjectLen = Integer.MAX_VALUE;
    jsonObjectsOverMaxLen = context.createCounter("jsonObjectsOverMaxLen");
  }

  @Override
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException {
    String sourceFile = file.getName();
    Reader reader = null;
    try {
      if (parser == null) {
        reader = new FileReader(file);
        parser = ((ContextExtensions) context).createJsonRecordReader(reader, offset, maxJsonObjectLen);
        reader = null;
      }
      offset = produce(sourceFile, offset, parser, maxBatchSize, batchMaker);
    } catch (OverrunException ex) {
      offset = -1;
      throw new BadSpoolFileException(file.getAbsolutePath(), ex.getStreamOffset(), ex);
    } catch (IOException ex) {
      offset = -1;
      long exOffset = (parser != null) ? parser.getPosition() : -1;
      throw new BadSpoolFileException(file.getAbsolutePath(), exOffset, ex);
    } finally {
      if (offset == -1) {
        if (parser != null) {
          try {
            parser.close();
          } catch (IOException ex) {
            //NOP
          }
          parser = null;
        }
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ex) {
            //NOP
          }
        }
      }
    }
    return offset;
  }

  protected long produce(String sourceFile, long offset, JsonRecordReader parser, int maxBatchSize,
      BatchMaker batchMaker) throws IOException {
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Record record = parser.readRecord();
        if (record != null) {
          batchMaker.addRecord(record);
          offset = parser.getPosition();
        } else {
          offset = -1;
          break;
        }
      } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
        jsonObjectsOverMaxLen.inc();
        context.reportError(Errors.SPOOLDIR_02, ex.getJsonSnippet(), maxJsonObjectLen, sourceFile, offset);
        LOG.warn(Errors.SPOOLDIR_02.getMessage(), ex.getJsonSnippet(), maxJsonObjectLen, sourceFile, offset);
      }
    }
    return offset;
  }

}
