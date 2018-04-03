/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.util.avroorc;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.util.orcsdc.OrcToSdcRecordConverter;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static com.streamsets.testing.Matchers.fieldWithValue;
import static com.streamsets.testing.Matchers.mapFieldWithEntry;
import static org.junit.Assert.assertThat;

public class TestAvroToOrcRecordConverter {

  private String createTempFile() {
    return String.format(
        "%s/output-%d.orc",
        System.getProperty("java.io.tmpdir"),
        Clock.systemUTC().millis()
    );
  }

  @Test
  public void basicConversion() throws IOException {
    AvroToOrcRecordConverter converter = new AvroToOrcRecordConverter(1000, new Properties(), new Configuration());

    Path outputFilePath = new Path(createTempFile());
    converter.convert(
        new SeekableFileInput(new File(TestAvroToOrcRecordConverter.class.getResource("users.avro").getFile())),
        outputFilePath
    );
  }

  @Test
  public void numericTypes() throws IOException {
    AvroToOrcRecordConverter converter = new AvroToOrcRecordConverter(1000, new Properties(), new Configuration());

    Path outputFilePath = new Path(createTempFile());
    converter.convert(
        new SeekableFileInput(new File(TestAvroToOrcRecordConverter.class.getResource("numeric-types.avro").getFile())),
        outputFilePath
    );

    try (OrcToSdcRecordConverter sdcRecordConverter = new OrcToSdcRecordConverter(outputFilePath)) {
      Record record1 = RecordCreator.create();
      boolean populated = sdcRecordConverter.populateRecord(record1);
      assertThat(populated, equalTo(true));

      assertThat(record1.get("/int"), fieldWithValue(42));
      assertThat(record1.get("/long"), fieldWithValue(478924424442112l));
      assertThat(record1.get("/float"), fieldWithValue(3.4f));
      assertThat(record1.get("/double"), fieldWithValue(4244875567.233d));
      assertThat(record1.get("/decimal"), fieldWithValue(new BigDecimal("1445.335")));
    }
  }

  @Test
  public void recordConversion() throws IOException {
    Path outputFilePath = new Path(createTempFile());

    Schema.Parser schemaParser = new Schema.Parser();
    Schema schema = schemaParser.parse(
        "{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"first\", \"type\": \"int\"},{" +
            "\"name\": \"second\", \"type\": {\"type\": \"record\", \"name\": \"MySubRecord\", \"fields\":" +
            " [{\"name\": \"sub1\", \"type\": \"string\"}, {\"name\": \"sub2\", \"type\": \"int\"}] } }, {\"name\":" +
            " \"somedate\", \"type\": { \"type\" : \"int\", \"logicalType\": \"date\"} } ]}"
    );

    TypeDescription orcSchema = AvroToOrcSchemaConverter.getOrcSchema(schema);

    Writer orcWriter = AvroToOrcRecordConverter.createOrcWriter(
        new Properties(),
        new Configuration(),
        outputFilePath,
        orcSchema
    );

    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("first", 1);
    avroRecord.put("somedate", 17535);

    Map<String, Object> subRecord = new HashMap<>();
    subRecord.put("sub1", new Utf8("value1"));
    subRecord.put("sub2", 42);

    avroRecord.put("second", subRecord);

    VectorizedRowBatch batch = orcSchema.createRowBatch();

    AvroToOrcRecordConverter.addAvroRecord(batch, avroRecord, orcSchema, 1000, orcWriter);
    orcWriter.addRowBatch(batch);
    batch.reset();
    orcWriter.close();

    // TODO: add code to read the ORC file and validate the contents
  }

  @Test
  public void allTypesRecordConversion() throws IOException {
    Path outputFilePath = new Path(createTempFile());

    Schema.Parser schemaParser = new Schema.Parser();
    Schema schema = schemaParser.parse(TestAvroToOrcRecordConverter.class.getResourceAsStream(
        "avro_schema_alltypes.json"
    ));

    TypeDescription orcSchema = AvroToOrcSchemaConverter.getOrcSchema(schema);

    Writer orcWriter = AvroToOrcRecordConverter.createOrcWriter(
        new Properties(),
        new Configuration(),
        outputFilePath,
        orcSchema
    );

    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("intField", 42);
    avroRecord.put("dateField", 17535);
    avroRecord.put("timeMillisField", 7844942);
    avroRecord.put("timeMicrosField", 57844942331l);
    avroRecord.put("timestampMillisField", 1515101663567l);
    avroRecord.put("timestampMicrosField", 1515101667468441l);
    avroRecord.put("decimalNoScaleField", new BigDecimal(7531));
    avroRecord.put("decimalField", new BigDecimal("12345678.5432"));
    avroRecord.put("unionField", 9876543210l);

    final HashMap<String, Double> mapFieldValue = new HashMap<>();
    mapFieldValue.put("first", 85.442d);
    mapFieldValue.put("second", 442.991d);
    mapFieldValue.put("third", -5.66d);
    avroRecord.put("mapField", mapFieldValue);

    final List<String> listValues = Arrays.asList("first_item", "second_item", "third_item");
    avroRecord.put("listField", listValues);

    Map<String, Object> subRecord = new HashMap<>();
    final String nestedStringValue = "nestedStringValue";
    subRecord.put("nestedStringField", new Utf8(nestedStringValue));
    subRecord.put("nestedFloatField", 43.21f);

    avroRecord.put("recordField", subRecord);

    VectorizedRowBatch batch = orcSchema.createRowBatch();

    AvroToOrcRecordConverter.addAvroRecord(batch, avroRecord, orcSchema, 1000, orcWriter);
    orcWriter.addRowBatch(batch);
    batch.reset();
    orcWriter.close();

    try (OrcToSdcRecordConverter sdcRecordConverter = new OrcToSdcRecordConverter(outputFilePath)) {

      Record record1 = RecordCreator.create();
      boolean populated = sdcRecordConverter.populateRecord(record1);
      assertThat(populated, equalTo(true));

      assertThat(record1.get("/intField"), fieldWithValue((int) avroRecord.get("intField")));

      // convert the "dateField" integer to equivalent java.util.Date, in terms of days from epoch
      final LocalDate dateField = LocalDate.ofEpochDay((int) avroRecord.get("dateField"));
      final Date adjusted = Date.from(dateField.atStartOfDay().atZone(ZoneOffset.UTC).toInstant());
      assertThat(record1.get("/dateField"), fieldWithValue(adjusted));

      assertThat(record1.get("/timeMillisField"), fieldWithValue((int) avroRecord.get("timeMillisField")));
      assertThat(record1.get("/timeMicrosField"), fieldWithValue((long) avroRecord.get("timeMicrosField")));

      final long timestampMillisField = (long) avroRecord.get("timestampMillisField");
      final LocalDateTime timestampMillisLocal = LocalDateTime.ofEpochSecond(
          timestampMillisField / 1000,
          1000000 * (int) (timestampMillisField % 1000),
          ZoneOffset.UTC
      );

      final ZonedDateTime timestampMillisZoned = ZonedDateTime.ofLocal(
          timestampMillisLocal,
          ZoneId.of(ZoneOffset.UTC.getId()),
          ZoneOffset.UTC
      );

      assertThat(record1.get("/timestampMillisField"), fieldWithValue(timestampMillisZoned));

      final long timestampMicrosField = (long) avroRecord.get("timestampMicrosField");
      final LocalDateTime timestampMicrosLocal = LocalDateTime.ofEpochSecond(
          timestampMicrosField / 1000000,
          1000 * (int) (timestampMicrosField % 1000000),
          ZoneOffset.UTC
      );

      final ZonedDateTime timestampMicrosZoned = ZonedDateTime.ofLocal(
          timestampMicrosLocal,
          ZoneId.of(ZoneOffset.UTC.getId()),
          ZoneOffset.UTC
      );

      assertThat(record1.get("/timestampMicrosField"), fieldWithValue(timestampMicrosZoned));

      assertThat(record1.get("/decimalNoScaleField"), fieldWithValue(
          (BigDecimal) avroRecord.get("decimalNoScaleField")
      ));

      assertThat(record1.get("/decimalField"), fieldWithValue((BigDecimal) avroRecord.get("decimalField")));

      assertThat(record1.get("/unionField"), fieldWithValue((long) avroRecord.get("unionField")));
      mapFieldValue.forEach((k, v) -> assertThat(record1.get("/mapField"), mapFieldWithEntry(k, v)));

      for (int i = 0; i < listValues.size(); i++) {
        assertThat(record1.get(String.format("/listField[%d]", i)), fieldWithValue(listValues.get(i)));
      }

      assertThat(record1.get("/recordField/nestedStringField"), fieldWithValue(nestedStringValue));
      assertThat(
          record1.get("/recordField/nestedFloatField"),
          fieldWithValue((Float) subRecord.get("nestedFloatField"))
      );
    }

  }
}
