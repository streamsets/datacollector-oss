/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class AvroDataFileParser implements DataParser {

  private static final String OFFSET_SEPARATOR = "::";

  private final Schema avroSchema;
  private final File file;
  private long previousSync;
  private long recordCount;
  private final DatumReader<GenericRecord> datumReader;
  private final DataFileReader<GenericRecord> dataFileReader;
  private boolean eof;
  private Stage.Context context;

  public AvroDataFileParser(Stage.Context context, String schema, File file, String readerOffset, int maxObjectLength)
    throws IOException {
    this.context = context;
    if(schema != null && !schema.isEmpty()) {
      avroSchema = new Schema.Parser().setValidate(true).parse(schema);
    } else {
      avroSchema = null;
    }
    this.file = file;
    datumReader = new GenericDatumReader<>(avroSchema, avroSchema, GenericData.get()); //Reader schema argument is optional
    dataFileReader = new DataFileReader<>(new SeekableOverrunFileInputStream(
      new FileInputStream(file), maxObjectLength, true), datumReader);
    if(readerOffset != null && !readerOffset.isEmpty() && !readerOffset.equals("0")) {
      String[] split = readerOffset.split(OFFSET_SEPARATOR);
      if(split.length == 3) {
        //split[0] is the file name
        previousSync = Long.parseLong(split[1]);
        recordCount = Long.parseLong(split[2]);
        seekToOffset();
      } else if (split.length == 2) {
        previousSync = Long.parseLong(split[0]);
        recordCount = Long.parseLong(split[1]);
        seekToOffset();
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid offset {}", readerOffset));
      }
    } else {
      recordCount = 0;
      previousSync = dataFileReader.previousSync();
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    //seekToOffset to the required position
    if(dataFileReader.hasNext()) {
      GenericRecord avroRecord = dataFileReader.next();
      if (dataFileReader.previousSync() > previousSync) {
        previousSync = dataFileReader.previousSync();
        recordCount = 1;
      } else {
        recordCount++;
      }
      Record record = context.createRecord(file.getName() + OFFSET_SEPARATOR + previousSync + OFFSET_SEPARATOR + recordCount);
      record.set(AvroTypeUtil.avroToSdcField(record, avroRecord.getSchema(), avroRecord));
      return record;
    }
    eof = true;
    return null;
  }

  private void seekToOffset() throws IOException {
    dataFileReader.seek(previousSync);
    int count = 0;
    while(count < recordCount) {
      if(dataFileReader.hasNext()) {
        dataFileReader.next();
        count++;
      } else {
        break;
      }
    }
  }

  @Override
  public String getOffset() throws DataParserException {
    return eof ? String.valueOf(-1) : String.valueOf(previousSync) + OFFSET_SEPARATOR + String.valueOf(recordCount);
  }

  @Override
  public void close() throws IOException {
    dataFileReader.close();
  }
}
