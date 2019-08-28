/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AvroEL {

  @ElFunction(
      prefix = "avro",
      name = "decode",
      description = "Returns an avro record using the specified schema and the byte array")
  public static GenericRecord decode(
      @ElParam("schema") String schema, @ElParam("bytes") byte[] avroBytes
  ) throws IOException {

    Schema.Parser parser = new Schema.Parser();
    Schema avroSchema = parser.parse(schema);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroBytes), null);
    GenericData.Record avroRecord = new GenericData.Record(avroSchema);

    return datumReader.read(avroRecord, decoder);
  }
}
