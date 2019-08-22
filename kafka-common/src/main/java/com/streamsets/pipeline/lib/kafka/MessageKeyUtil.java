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
import com.streamsets.pipeline.api.base.Errors;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.stage.origin.kafka.KeyCaptureMode;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.EnumSet;

public class MessageKeyUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MessageKeyUtil.class);

  public static final String AVRO_KEY_SCHEMA_HEADER = "avroKeySchema";

  public static void handleMessageKey(
      Object messageKey,
      KeyCaptureMode keyCaptureMode,
      Record targetRecord,
      String keyCaptureField,
      String keyCaptureAttribute
  ) {
    if (messageKey != null) {
      final MessageKeyResult result = new MessageKeyResult(messageKey, keyCaptureMode, targetRecord);

      final EnumSet<KeyCaptureMode> fieldModes = EnumSet.of(
          KeyCaptureMode.RECORD_FIELD,
          KeyCaptureMode.RECORD_HEADER_AND_FIELD
      );
      final EnumSet<KeyCaptureMode> headerModes = EnumSet.of(
          KeyCaptureMode.RECORD_HEADER,
          KeyCaptureMode.RECORD_HEADER_AND_FIELD
      );
      final GenericRecord avroRecord = result.messageKeyRecord;
      if (fieldModes.contains(keyCaptureMode)) {
        if (result.messageKeyStr != null) {
          targetRecord.set(keyCaptureField, Field.create(result.messageKeyStr));
        } else if (result.messageKeyBytes != null) {
          targetRecord.set(keyCaptureField, Field.create(result.messageKeyBytes));
        } else if (avroRecord != null) {
          final Field messageKeyField = AvroTypeUtil.avroToSdcField(
              // since we skipAvroUnionIndexes (last arg=true), this parameter isn't actually used
              targetRecord,
              ((GenericData.Record) messageKey).getSchema(),
              messageKey,
              true
          );
          targetRecord.set(keyCaptureField, messageKeyField);
        }
      }
      if (headerModes.contains(keyCaptureMode)) {
        if (result.messageKeyStr != null) {
          targetRecord.getHeader().setAttribute(keyCaptureAttribute, result.messageKeyStr);
        } else if (result.messageKeyBytes != null) {
          targetRecord.getHeader().setAttribute(keyCaptureAttribute, Base64.getEncoder().encodeToString(
              result.messageKeyBytes
          ));
        } else if (avroRecord != null) {
          final byte[] encodedKey;
          try {
            encodedKey = AvroTypeUtil.getBinaryEncodedAvroRecord(avroRecord);
          } catch (IOException e) {
            throw new OnRecordErrorException(
                targetRecord,
                KafkaErrors.KAFKA_202,
                e.getClass().getSimpleName(),
                e.getMessage(),
                e
            );
          }

          targetRecord.getHeader().setAttribute(keyCaptureAttribute, Base64.getEncoder().encodeToString(encodedKey));
          targetRecord.getHeader().setAttribute(AVRO_KEY_SCHEMA_HEADER, avroRecord.getSchema().toString());
        }
      }
    }
  }

  private static class MessageKeyResult {
    private final Object messageKey;
    private final String messageKeyStr;
    private final byte[] messageKeyBytes;
    private final GenericRecord messageKeyRecord;

    private MessageKeyResult(
        Object messageKey,
        KeyCaptureMode keyCaptureMode,
        Record targetRecord
    ) {
      Utils.checkNotNull(messageKey, "messageKey");
      this.messageKey = messageKey;

      // TODO: check whether Avro top level numeric types turn directly into Java types
      if (messageKey instanceof String) {
        messageKeyStr = (String) messageKey;
        messageKeyBytes = null;
        messageKeyRecord = null;
      } else if (messageKey instanceof byte[]) {
        messageKeyStr = null;
        messageKeyBytes = (byte[]) messageKey;
        messageKeyRecord = null;
      } else if (messageKey instanceof GenericData.Record) {
        messageKeyStr = null;
        messageKeyBytes = null;
        messageKeyRecord = (GenericRecord) messageKey;
      } else {
        throw new IllegalStateException(String.format(
            "Unrecognized key type: %s",
            messageKey.getClass().getName()
        ));
      }
    }
  }
}
