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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.util.orcsdc.OrcToSdcRecordConverter;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.testing.Matchers;
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
import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.streamsets.testing.Matchers.dateFieldWithValue;
import static com.streamsets.testing.Matchers.listFieldWithValues;
import static com.streamsets.testing.Matchers.zonedDateTimeUTCFieldWithValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static com.streamsets.testing.Matchers.fieldWithValue;
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

    GenericData.Record subRecord = new GenericData.Record(schema.getField("second").schema());
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
  public void unionTypeConversions() throws IOException {
    final Path outputFilePath = new Path(createTempFile());

    final Schema.Parser schemaParser = new Schema.Parser();
    final Schema schema = schemaParser.parse(TestAvroToOrcRecordConverter.class.getResourceAsStream("avro_union_types.json"));

    final TypeDescription orcSchema = AvroToOrcSchemaConverter.getOrcSchema(schema);

    final Writer orcWriter = AvroToOrcRecordConverter.createOrcWriter(
        new Properties(),
        new Configuration(),
        outputFilePath,
        orcSchema
    );

    final GenericRecord avroRecord1 = new GenericData.Record(schema);
    avroRecord1.put("nullableInteger", 87);
    avroRecord1.put("integerOrString", "someString");
    avroRecord1.put("nullableStringOrInteger", "nonNullString");
    avroRecord1.put("justLong", 57844942331l);

    final GenericRecord avroRecord2 = new GenericData.Record(schema);
    avroRecord2.put("nullableInteger", null);
    avroRecord2.put("integerOrString", 16);
    avroRecord2.put("nullableStringOrInteger", null);
    avroRecord2.put("justLong", 758934l);

    final VectorizedRowBatch batch = orcSchema.createRowBatch();

    AvroToOrcRecordConverter.addAvroRecord(batch, avroRecord1, orcSchema, 1000, orcWriter);
    AvroToOrcRecordConverter.addAvroRecord(batch, avroRecord2, orcSchema, 1000, orcWriter);
    orcWriter.addRowBatch(batch);
    batch.reset();
    orcWriter.close();

    try (OrcToSdcRecordConverter sdcRecordConverter = new OrcToSdcRecordConverter(outputFilePath)) {

      final Record record1 = RecordCreator.create();
      boolean populated = sdcRecordConverter.populateRecord(record1);
      assertThat(populated, equalTo(true));
      assertSdcRecordMatchesAvro(record1, avroRecord1, null);

      final Record record2 = RecordCreator.create();
      populated = sdcRecordConverter.populateRecord(record2);
      assertThat(populated, equalTo(true));
      assertSdcRecordMatchesAvro(
          record2,
          avroRecord2,
          ImmutableMap.<String, Matcher<Field>>builder()
              .put("nullableInteger", Matchers.intFieldWithNullValue())
              .put("nullableStringOrInteger", Matchers.stringFieldWithNullValue())
              .build()
      );
    }
  }

  @Test
  public void complexConversion() throws IOException {

    final Path outputFilePath = new Path(createTempFile());

    final AvroToOrcRecordConverter converter = new AvroToOrcRecordConverter(
        1000,
        new Properties(),
        new Configuration()
    );

    converter.convert(
        new SeekableFileInput(new File(TestAvroToOrcRecordConverter.class.getResource("complex.avro").getFile())),
        new Path(outputFilePath.toString())
    );

    try (OrcToSdcRecordConverter sdcRecordConverter = new OrcToSdcRecordConverter(outputFilePath)) {
      final Record record1 = RecordCreator.create();
      boolean populated = sdcRecordConverter.populateRecord(record1);
      assertThat(populated, equalTo(true));

      assertThat(record1.get("/intField"), fieldWithValue(42));
      assertThat(record1.get("/recordField/nestedStringField"), fieldWithValue("foo"));
      assertThat(record1.get("/recordField/nestedFloatField"), fieldWithValue(98.6f));
      assertThat(record1.get("/recordField/nestedStringOrLong"), fieldWithValue("I'm a string!"));
      assertThat(record1.get("/dateField"), dateFieldWithValue(new Date(1468800000l)));
      assertThat(record1.get("/timeMillisField"), fieldWithValue(400));
      assertThat(record1.get("/timeMicrosField"), fieldWithValue(8000l));
      assertThat(record1.get("/timestampMillisField"), zonedDateTimeUTCFieldWithValue(788553l, 45000000l));
      assertThat(record1.get("/timestampMicrosField"), zonedDateTimeUTCFieldWithValue(34l, 456630000l));
      assertThat(record1.get("/decimalField"), fieldWithValue(new BigDecimal("1889.33")));
      assertThat(record1.get("/unionField"), fieldWithValue(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
      assertThat(record1.get("/mapOfDoublesField/first"), fieldWithValue(100.5d));
      assertThat(record1.get("/mapOfDoublesField/second"), fieldWithValue(202.1d));
      assertThat(record1.get("/listOfStringsField"), listFieldWithValues("firstListItem", "secondListItem"));
      assertThat(record1.get("/enumField"), fieldWithValue("THAT"));

      final Record record2 = RecordCreator.create();
      populated = sdcRecordConverter.populateRecord(record2);
      assertThat(populated, equalTo(true));

      assertThat(record2.get("/intField"), fieldWithValue(-783));
      assertThat(record2.get("/recordField/nestedStringField"), fieldWithValue("bar"));
      assertThat(record2.get("/recordField/nestedFloatField"), fieldWithValue(-15.3f));
      assertThat(record2.get("/recordField/nestedStringOrLong"), fieldWithValue(678l));
      assertThat(record2.get("/dateField"), dateFieldWithValue(new Date(1209600000l)));
      assertThat(record2.get("/timeMillisField"), fieldWithValue(993));
      assertThat(record2.get("/timeMicrosField"), fieldWithValue(12300l));
      assertThat(record2.get("/timestampMillisField"), zonedDateTimeUTCFieldWithValue(177708l, 109000000l));
      assertThat(record2.get("/timestampMicrosField"), zonedDateTimeUTCFieldWithValue(1192l, 408542000l));
      assertThat(record2.get("/decimalField"), fieldWithValue(new BigDecimal("-1981.3342")));
      assertThat(record2.get("/unionField"), fieldWithValue(new byte[]{1, 1, 2, 3, 5, 8, 13, 21}));
      assertThat(record2.get("/mapOfDoublesField/otherFirst"), fieldWithValue(-33.09d));
      assertThat(record2.get("/mapOfDoublesField/whatever"), fieldWithValue(18933.40d));
      assertThat(record2.get("/listOfStringsField"), listFieldWithValues("otherList1", "otherList2", "otherList3"));
      assertThat(record2.get("/enumField"), fieldWithValue("THE_OTHER"));
    }
  }

  private static void assertSdcRecordMatchesAvro(
      final Record sdcRecord,
      final GenericRecord avroRecord,
      final Map<String, Matcher<Field>> nullFieldMatchers
  ) {
    for (final Schema.Field field : avroRecord.getSchema().getFields()) {
      final Object avroValue = avroRecord.get(field.name());
      final Field sdcField = sdcRecord.get("/" + field.name());
      if (avroValue != null) {
        assertThat(sdcField, fieldWithValue(avroValue));
      } else {
        final Matcher<Field> matcher = nullFieldMatchers.get(field.name());
        assertThat(sdcField, matcher);
      }
    }
  }
}
