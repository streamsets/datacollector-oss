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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Field Hasher")
public class FieldHasherProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(label = "Fields to hash", required = true, type = Type.MODEL, defaultValue="",
    description="The fields whose values must be replaced by their SHA values.")
  @FieldSelector
  public List<String> fields;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Record recordClone = getContext().cloneRecord(record);
    for (String fieldToHash : fields) {
      Field field = recordClone.get(fieldToHash);
      field.set(Field.Type.STRING, generateHashForField(field));
    }
    batchMaker.addRecord(recordClone);
  }

  private String generateHashForField(Field field) {
    String valueAsString = getValueAsString(field);
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
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
    if(field.getType()== Field.Type.BOOLEAN) {
      return String.valueOf(field.getValueAsBoolean());
    } else if(field.getType()== Field.Type.BYTE) {
      return String.valueOf(field.getValueAsByte());
    } else if(field.getType()== Field.Type.BYTE_ARRAY) {
      return String.valueOf(field.getValueAsByteArray());
    } else if(field.getType()== Field.Type.CHAR) {
      return String.valueOf(field.getValueAsChar());
    } else if(field.getType()== Field.Type.DATE) {
      return String.valueOf(field.getValueAsDate());
    } else if(field.getType()== Field.Type.DATETIME) {
      return String.valueOf(field.getValueAsDatetime());
    } else if(field.getType()== Field.Type.DECIMAL) {
      return String.valueOf(field.getValueAsDecimal());
    } else if(field.getType()== Field.Type.DOUBLE) {
      return String.valueOf(field.getValueAsDouble());
    } else if(field.getType()== Field.Type.FLOAT) {
      return String.valueOf(field.getValueAsFloat());
    } else if(field.getType()== Field.Type.INTEGER) {
      return String.valueOf(field.getValueAsInteger());
    } else if(field.getType()== Field.Type.LONG) {
      return String.valueOf(field.getValueAsLong());
    } else if(field.getType()== Field.Type.SHORT) {
      return String.valueOf(field.getValueAsShort());
    } else if(field.getType()== Field.Type.STRING) {
      return String.valueOf(field.getValueAsString());
    }
    return null;
  }

}
