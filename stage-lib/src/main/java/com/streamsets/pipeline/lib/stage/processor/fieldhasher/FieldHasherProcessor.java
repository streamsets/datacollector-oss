/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Field Hasher")
public class FieldHasherProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldHasherProcessor.class);
  private static final String SHA_256 = "SHA-256";

  @ConfigDef(label = "Fields to hash", required = true, type = Type.MODEL, defaultValue="",
    description="The fields whose values must be replaced by their SHA values. Non string values will be " +
      "converted to String values for has computation. Fields with Map and List types will be ignored.")
  @FieldSelector
  public List<String> fields;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    for (String fieldToHash : fields) {
      if(record.has(fieldToHash)) {
        Field field = record.get(fieldToHash);
        if (field.getType() == Field.Type.MAP || field.getType() == Field.Type.LIST) {
          LOG.info("The field {} in record {} is of type {}. Ignoring field.", fieldToHash,
            record.getHeader().getSourceId(), field.getType().name());
        } else if(field.getValue() == null) {
          LOG.info("The field {} in record {} has null value. Ignoring field.", fieldToHash,
            record.getHeader().getSourceId());
        } else {
          Field newField = Field.create(generateHashForField(field));
          record.set(fieldToHash, newField);
        }
      } else {
        LOG.info("Could not find field {} in record {}.", fieldToHash, record.getHeader().getSourceId());
      }
    }
    batchMaker.addRecord(record);
  }

  private String generateHashForField(Field field) throws StageException {
    String valueAsString = getValueAsString(field);
    if(valueAsString == null) {
      return null;
    }
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance(SHA_256);
    } catch (NoSuchAlgorithmException e) {
      LOG.error(StageLibError.LIB_0500.getMessage(), SHA_256, e.getMessage());
      throw new StageException(StageLibError.LIB_0500, SHA_256, e.getMessage(), e);
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
