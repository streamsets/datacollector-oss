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
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

public class AvroMessageParser extends AbstractDataParser {

  private final OriginAvroSchemaSource schemaSource;
  private DatumReader<GenericRecord> datumReader;
  private DataFileReader<GenericRecord> dataFileReader;
  private BinaryDecoder decoder;
  private GenericData.Record avroRecord;
  private boolean eof;
  private final ProtoConfigurableEntity.Context context;
  private final String messageId;

  public AvroMessageParser(
      ProtoConfigurableEntity.Context context,
      final Schema schema,
      final byte[] message,
      final String messageId,
      final OriginAvroSchemaSource schemaSource
  ) throws IOException {
    this.context = context;
    this.messageId = messageId;
    this.schemaSource = schemaSource;

    datumReader = new GenericDatumReader<>(schema); //Reader schema argument is optional
    if(schemaSource == OriginAvroSchemaSource.SOURCE) {
      dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(message), datumReader);
    } else {
      decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(message), null);
      avroRecord = new GenericData.Record(schema);
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    GenericRecord genericRecord;
    Record record = null;
    if(schemaSource == OriginAvroSchemaSource.SOURCE) {
      genericRecord = parseMessageWithSchema();
    } else {
      genericRecord = parseMessageWithoutSchema();
    }
    if(genericRecord != null) {
      record = context.createRecord(messageId);
      record.set(AvroTypeUtil.avroToSdcField(record, genericRecord.getSchema(), genericRecord));
      record.getHeader().setAttribute(HeaderAttributeConstants.AVRO_SCHEMA, genericRecord.getSchema().toString());
    }
    return record;
  }

  private GenericRecord parseMessageWithSchema() {
    if(dataFileReader.hasNext()) {
      return dataFileReader.next();
    }
    eof = true;
    return null;
  }

  private GenericRecord parseMessageWithoutSchema() throws IOException {
    try {
      return datumReader.read(avroRecord, decoder);
    } catch (EOFException e) {
      eof = true;
    }
    return null;
  }

  @Override
  public String getOffset() throws DataParserException {
    return eof ? String.valueOf(-1) : messageId;
  }

  @Override
  public void close() throws IOException {
    if(dataFileReader != null) {
      dataFileReader.close();
    }
  }

}
