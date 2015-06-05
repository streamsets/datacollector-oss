/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
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

public class AvroMessageParser implements DataParser {

  private Schema avroSchema;
  private DatumReader<GenericRecord> datumReader;
  private DataFileReader<GenericRecord> dataFileReader;
  private BinaryDecoder decoder;
  private GenericData.Record avroRecord;
  private boolean eof;
  private Stage.Context context;
  private final String messageId;
  private final boolean messageHasSchema;

  public AvroMessageParser(Stage.Context context, String schema, byte[] message, String messageId,
                           boolean messageHasSchema) throws IOException {
    this.context = context;
    this.messageId = messageId;
    this.messageHasSchema = messageHasSchema;
    if(messageHasSchema) {
      if(schema != null && !schema.isEmpty()) {
        avroSchema = new Schema.Parser().setValidate(true).parse(schema);
      }
      datumReader = new GenericDatumReader<>(avroSchema); //Reader schema argument is optional
      dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(message), datumReader);
    } else {
      avroSchema = new Schema.Parser().setValidate(true).parse(schema);
      datumReader = new GenericDatumReader<>(avroSchema); //Reader schema argument is optional
      decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(message), null);
      avroRecord = new GenericData.Record(avroSchema);
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    GenericRecord genericRecord;
    Record record = null;
    if(messageHasSchema) {
      genericRecord = parseMessageWithSchema();
    } else {
      genericRecord = parseMessageWithoutSchema();
    }
    if(genericRecord != null) {
      record = context.createRecord(messageId);
      record.set(AvroTypeUtil.avroToSdcField(record, genericRecord.getSchema(), genericRecord));
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
