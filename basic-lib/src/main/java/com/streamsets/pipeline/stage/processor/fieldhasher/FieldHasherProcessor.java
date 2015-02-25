/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.config.OnStagePreConditionFailureChooserValues;
import com.streamsets.pipeline.stage.util.StageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Field Hasher",
    description = "Uses an algorithm to hash field values",
    icon="hash.png")
@ConfigGroups(Groups.class)
public class FieldHasherProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldHasherProcessor.class);

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "",
      description="",
      displayPosition = 10,
      group = "HASHING"
  )
  @ComplexField
  public List<FieldHasherConfig> fieldHasherConfigs;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "TO_ERROR",
    label = "On Field Issue",
    description="Action for data that does not contain the specified fields, the field value is null or if the " +
      "field type is Map or List",
    displayPosition = 20,
    group = "HASHING"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = OnStagePreConditionFailureChooserValues.class)
  public OnStagePreConditionFailure onStagePreConditionFailure;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldsDontExist = new HashSet<>();
    Set<String> fieldsWithListOrMapType = new HashSet<>();
    Set<String> fieldsWithNull = new HashSet<>();

    for(FieldHasherConfig fieldHasherConfig : fieldHasherConfigs) {
      for(String fieldToHash : fieldHasherConfig.fieldsToHash) {
        if(record.has(fieldToHash)) {
          Field field = record.get(fieldToHash);
          if (field.getType() == Field.Type.MAP || field.getType() == Field.Type.LIST) {
            fieldsWithListOrMapType.add(fieldToHash);
          } else if(field.getValue() == null) {
            fieldsWithNull.add(fieldToHash);
          } else {
            Field newField = Field.create(generateHashForField(field, fieldHasherConfig.hashType));
            record.set(fieldToHash, newField);
          }
        } else {
          fieldsDontExist.add(fieldToHash);
        }
      }
    }

    if(onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR) {
      throw new OnRecordErrorException(Errors.HASH_01, record.getHeader().getSourceId(),
        StageUtil.getCommaSeparatedNames(fieldsDontExist),  StageUtil.getCommaSeparatedNames(fieldsWithNull),
        StageUtil.getCommaSeparatedNames(fieldsWithListOrMapType));
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
      throw new StageException(Errors.HASH_00, hashType.getDigest(), e.getMessage(), e);
    }
    messageDigest.update(valueAsString.getBytes());
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
      return new String(field.getValueAsByteArray());
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
