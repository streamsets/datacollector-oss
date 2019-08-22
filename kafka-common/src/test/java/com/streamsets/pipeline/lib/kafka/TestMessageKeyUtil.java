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

package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.WholeFileChecksumAlgorithm;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.origin.kafka.KeyCaptureMode;
import com.streamsets.testing.Matchers;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static com.streamsets.testing.Matchers.fieldWithValue;

@RunWith(Parameterized.class)
public class TestMessageKeyUtil {

  private final Object messageKey;
  private final KeyCaptureMode keyCaptureMode;
  private final String keyCaptureField;
  private final String keyCaptureAttribute;
  private final Function<Record, Record> recordVerifier;

  public TestMessageKeyUtil(
      Object messageKey,
      KeyCaptureMode keyCaptureMode,
      String keyCaptureField,
      String keyCaptureAttribute,
      Function<Record, Record> recordVerifier
  ) {
    this.messageKey = messageKey;
    this.keyCaptureMode = keyCaptureMode;
    this.keyCaptureField = keyCaptureField;
    this.keyCaptureAttribute = keyCaptureAttribute;
    this.recordVerifier = recordVerifier;
  }

  @Parameterized.Parameters(name = "Message Key: {0}, Key Capture Mode: {1}, Key Capture Field: {2}, Key Capture" +
      " Attribute: {3}")
  public static Collection<Object[]> testData() throws Exception {
    List<Object[]> finalData = new ArrayList<>();

    final String stringKeyVal = "stringKey";
    final byte[] binaryKeyVal = new byte[] {1, 1, 2, 3, 5, 8, 13, 21, 34, 55};

    final String avroKeyField1 = "k1";
    final String avroKeyField2 = "k2";
    final long avroKeyField1Value = 42l;
    final int avroKeyField2Value = 8;

    final GenericData.Record avroRecordKeyVal = new GenericData.Record(
        SchemaBuilder
            .record("KeyRecordType")
            .fields()
            .name(avroKeyField1).type().longType().noDefault()
            .name(avroKeyField2).type().intType().noDefault()
            .endRecord()
    );
    avroRecordKeyVal.put(avroKeyField1, avroKeyField1Value);
    avroRecordKeyVal.put(avroKeyField2, avroKeyField2Value);

    final String fieldName = "/messageKeyOutputField";
    final String attributeName = "messageKeyOutputAttribute";

    final Function<Record, Record> verifyStringHeader = record -> {
      assertThat(record.getHeader().getAttribute(attributeName), equalTo(stringKeyVal));
      return record;
    };

    final Function<Record, Record> verifyBinaryHeader = record -> {
      final String attribute = record.getHeader().getAttribute(attributeName);
      assertThat(attribute, notNullValue());
      assertThat(Base64.getDecoder().decode(attribute), equalTo(binaryKeyVal));
      return record;
    };

    final Function<Record, Record> verifyBinaryField = record -> {
      assertThat(record.get(fieldName), Matchers.fieldWithValue(binaryKeyVal));
      return record;
    };

    final Function<Record, Record> verifyAvroField = record -> {
      assertThat(record.get(fieldName), Matchers.mapFieldWithEntry(avroKeyField1, avroKeyField1Value));
      assertThat(record.get(fieldName), Matchers.mapFieldWithEntry(avroKeyField2, avroKeyField2Value));
      return record;
    };

    final Function<Record, Record> verifyBase64EncodedAvroRecordInHeader = record -> {
      final String attrVal = record.getHeader().getAttribute(attributeName);
      assertThat(attrVal, notNullValue());
      final byte[] bytes = Base64.getDecoder().decode(attrVal);
      try {
        final GenericRecord decoded = AvroTypeUtil.getAvroRecordFromBinaryEncoding(avroRecordKeyVal.getSchema(), bytes);
        assertThat(decoded, equalTo(avroRecordKeyVal));
      } catch (IOException e) {
        throw new RuntimeException("IOException encountered during getAvroRecordFromBinaryEncoding", e);
      }
      return record;
    };


    finalData.add(new Object[] {
        stringKeyVal,
        KeyCaptureMode.RECORD_HEADER,
        fieldName,
        attributeName,
        verifyStringHeader
    });

    finalData.add(new Object[] {
        binaryKeyVal,
        KeyCaptureMode.RECORD_HEADER_AND_FIELD,
        fieldName,
        attributeName,
        verifyBinaryHeader.andThen(verifyBinaryField)
    });

    finalData.add(new Object[] {
        avroRecordKeyVal,
        KeyCaptureMode.RECORD_FIELD,
        fieldName,
        attributeName,
        verifyAvroField
    });

    finalData.add(new Object[] {
        avroRecordKeyVal,
        KeyCaptureMode.RECORD_HEADER,
        fieldName,
        attributeName,
        verifyBase64EncodedAvroRecordInHeader
    });

    return finalData;
  }

  @Test
  @SuppressWarnings("ReturnValueIgnored")
  public void testHandleMessageKey() {
    final Record testRecord = RecordCreator.create();
    testRecord.set(Field.createListMap(new LinkedHashMap<>()));

    MessageKeyUtil.handleMessageKey(
        messageKey,
        keyCaptureMode,
        testRecord,
        keyCaptureField,
        keyCaptureAttribute
    );

    recordVerifier.apply(testRecord);
  }
}
