/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.AvroJavaSnappyCodec;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class AvroDataFileParser extends AbstractDataParser {

  static {
    // replace Avro Snappy codec with SDC's which is 100% Java
    AvroJavaSnappyCodec.initialize();
  }

  private static final String OFFSET_SEPARATOR = "::";

  private final File file;
  private final SeekableOverrunFileInputStream sin;
  private long previousSync;
  private long recordCount;
  private final DataFileReader<GenericRecord> dataFileReader;
  private boolean eof;
  private ProtoConfigurableEntity.Context context;

  public AvroDataFileParser(ProtoConfigurableEntity.Context context, Schema schema, File file, String readerOffset, int maxObjectLength)
    throws IOException {
    this.context = context;
    this.file = file;
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema, schema, GenericData.get());
    sin = new SeekableOverrunFileInputStream(
      new FileInputStream(file), maxObjectLength, true);
    dataFileReader = new DataFileReader<>(sin, datumReader);
    if(readerOffset != null && !readerOffset.isEmpty() && !"0".equals(readerOffset)) {
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
      sin.resetCount();
      if (dataFileReader.previousSync() > previousSync) {
        previousSync = dataFileReader.previousSync();
        recordCount = 0;
      }
      GenericRecord avroRecord = dataFileReader.next();
      recordCount++;
      Record record = context.createRecord(file.getName() + OFFSET_SEPARATOR + previousSync + OFFSET_SEPARATOR + recordCount);
      record.set(AvroTypeUtil.avroToSdcField(record, avroRecord.getSchema(), avroRecord));
      record.getHeader().setAttribute(HeaderAttributeConstants.AVRO_SCHEMA, avroRecord.getSchema().toString());
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
        sin.resetCount();
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
