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
import com.streamsets.pipeline.lib.io.OverrunInputStream;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.io.InputStream;

public class AvroDataStreamParser extends AbstractDataParser {

  private static final String OFFSET_SEPARATOR = "::";

  private final Schema avroSchema;
  private final String streamName;
  private long recordCount;
  private final DatumReader<GenericRecord> datumReader;
  private final DataFileStream<GenericRecord> dataFileStream;
  private final OverrunInputStream overrunInputStream;
  private boolean eof;
  private ProtoConfigurableEntity.Context context;

  public AvroDataStreamParser(
      ProtoConfigurableEntity.Context context,
      Schema schema,
      String streamName,
      InputStream inputStream,
      long recordCount,
      int maxObjectLength
  ) throws IOException {
    this.context = context;
    avroSchema = schema;
    this.streamName = streamName;
    this.recordCount = recordCount;
    datumReader = new GenericDatumReader<>(avroSchema, avroSchema, GenericData.get()); //Reader schema argument is optional
    overrunInputStream = new OverrunInputStream(inputStream, maxObjectLength, true);
    dataFileStream = new DataFileStream<>(overrunInputStream, datumReader);
    seekToOffset();
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    //seekToOffset to the required position
    if(dataFileStream.hasNext()) {
      //reset count for the next object.
      //maxObjectLength indicates that a single record should not exceed the specified limit. Max 1 MB.
      //The file itself may contain multiple large records and the total file size may be over maxObjectLength
      overrunInputStream.resetCount();

      GenericRecord avroRecord = dataFileStream.next();
      recordCount++;
      Record record = context.createRecord(streamName + OFFSET_SEPARATOR + recordCount);
      record.set(AvroTypeUtil.avroToSdcField(record, avroRecord.getSchema(), avroRecord));
      record.getHeader().setAttribute(HeaderAttributeConstants.AVRO_SCHEMA, avroRecord.getSchema().toString());
      return record;
    }
    eof = true;
    return null;
  }

  private void seekToOffset() throws IOException {
    int count = 0;
    while(count < recordCount) {
      if(dataFileStream.hasNext()) {
        overrunInputStream.resetCount();
        dataFileStream.next();
        count++;
      } else {
        break;
      }
    }
  }

  @Override
  public String getOffset() throws DataParserException {
    return eof ? String.valueOf(-1) : String.valueOf(recordCount);
  }

  @Override
  public void close() throws IOException {
    dataFileStream.close();
  }
}
