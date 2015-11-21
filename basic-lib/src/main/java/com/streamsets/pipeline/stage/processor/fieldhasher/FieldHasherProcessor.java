/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldHasherProcessor extends SingleLaneRecordProcessor {
  private final List<FieldHasherConfig> fieldHasherConfigs;
  private final OnStagePreConditionFailure onStagePreConditionFailure;

  public FieldHasherProcessor(
      List<FieldHasherConfig> fieldHasherConfigs,
      OnStagePreConditionFailure onStagePreConditionFailure) {
    this.fieldHasherConfigs = fieldHasherConfigs;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getFieldPaths();
    Set<String> fieldsDontExist = new HashSet<>();
    Set<String> fieldsWithListOrMapType = new HashSet<>();
    Set<String> fieldsWithNull = new HashSet<>();

    for(FieldHasherConfig fieldHasherConfig : fieldHasherConfigs) {
      for(String fieldToHash : fieldHasherConfig.fieldsToHash) {
        for(String matchingFieldPath : FieldRegexUtil.getMatchingFieldPaths(fieldToHash, fieldPaths)) {
          if (record.has(matchingFieldPath)) {
            Field field = record.get(matchingFieldPath);
            if (field.getType() == Field.Type.MAP || field.getType() == Field.Type.LIST || field.getType() == Field.Type.LIST_MAP) {
              fieldsWithListOrMapType.add(matchingFieldPath);
            } else if (field.getValue() == null) {
              fieldsWithNull.add(matchingFieldPath);
            } else {
              Field newField = Field.create(generateHashForField(field, fieldHasherConfig.hashType));
              record.set(matchingFieldPath, newField);
            }
          } else {
            fieldsDontExist.add(matchingFieldPath);
          }
        }
      }
    }

    if(onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR
      && !(fieldsDontExist.isEmpty() && fieldsWithListOrMapType.isEmpty() && fieldsWithNull.isEmpty())) {
      throw new OnRecordErrorException(Errors.HASH_01, record.getHeader().getSourceId(),
        Joiner.on(", ").join(fieldsDontExist),  Joiner.on(", ").join(fieldsWithNull),
        Joiner.on(", ").join(fieldsWithListOrMapType));
    }
    batchMaker.addRecord(record);
  }

  private String generateHashForField(Field field, HashType hashType) throws StageException {
    String valueAsString = getValueAsString(field);
    if(valueAsString == null) {
      return null;
    }
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance(hashType.getDigest());
    } catch (NoSuchAlgorithmException e) {
      throw new StageException(Errors.HASH_00, hashType.getDigest(), e.toString(), e);
    }
    messageDigest.update(valueAsString.getBytes(StandardCharsets.UTF_8));
    byte byteData[] = messageDigest.digest();

    //encode byte[] into hex
    StringBuilder sb = new StringBuilder();
    for(byte b : byteData) {
      sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
    }
    return sb.toString();
  }

  private String getValueAsString(Field field) {
    if (field.getType() == Field.Type.BOOLEAN) {
      return String.valueOf(field.getValueAsBoolean());
    } else if (field.getType() == Field.Type.BYTE) {
      return String.valueOf(field.getValueAsByte());
    } else if (field.getType() == Field.Type.BYTE_ARRAY) {
      return new String(field.getValueAsByteArray(), StandardCharsets.UTF_8);
    } else if (field.getType() == Field.Type.CHAR) {
      return String.valueOf(field.getValueAsChar());
    } else if (field.getType() == Field.Type.DATE) {
      return String.valueOf(field.getValueAsDate());
    } else if (field.getType() == Field.Type.DATETIME) {
      return String.valueOf(field.getValueAsDatetime());
    } else if (field.getType() == Field.Type.DECIMAL) {
      return String.valueOf(field.getValueAsDecimal());
    } else if (field.getType() == Field.Type.DOUBLE) {
      return String.valueOf(field.getValueAsDouble());
    } else if (field.getType() == Field.Type.FLOAT) {
      return String.valueOf(field.getValueAsFloat());
    } else if (field.getType() == Field.Type.INTEGER) {
      return String.valueOf(field.getValueAsInteger());
    } else if (field.getType() == Field.Type.LONG) {
      return String.valueOf(field.getValueAsLong());
    } else if (field.getType() == Field.Type.SHORT) {
      return String.valueOf(field.getValueAsShort());
    } else if (field.getType() == Field.Type.STRING) {
      return String.valueOf(field.getValueAsString());
    }
    return null;
  }

}
