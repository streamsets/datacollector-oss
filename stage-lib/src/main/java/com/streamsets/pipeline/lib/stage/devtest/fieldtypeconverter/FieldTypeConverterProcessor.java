/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Dummy Field Type Converter")
public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(label = "String fields to convert", required = false,type = Type.MODEL, defaultValue="")
  @FieldValueChooser(type= ChooserMode.PROVIDED, chooserValues = ConverterValuesProvider.class)
  public Map<String, FieldType> fields;

  //FIXME<Hari>: Do we need a drop down to select date and date time formats?

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Record recordClone = getContext().cloneRecord(record);

    for(Map.Entry<String, FieldType> field : fields.entrySet()) {
      String fieldName = field.getKey();
      Field f = recordClone.get(fieldName);
      String stringValue = f.getValueAsString();
      Object value = convertToType(stringValue, field.getValue());
      Field newFiled = Field.create(f, value);
      recordClone.delete(fieldName);
      recordClone.set(newFiled);
    }

    batchMaker.addRecord(recordClone);
  }

  private Object convertToType(String stringValue, FieldType fieldType) {
    switch(fieldType) {
      case BOOLEAN:
        return Boolean.valueOf(stringValue);
      case BYTE:
        return Byte.valueOf(stringValue);
      case BYTE_ARRAY:
        return stringValue.getBytes();
      case CHAR:
        return stringValue.charAt(0);
      case DATE:
        DateFormat dateFormat = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH);
        Date date;
        try {
          date = dateFormat.parse(stringValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return date;
      case DATETIME:
        DateFormat dateTimeFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.ENGLISH);
        Date dateTime;
        try {
          dateTime = dateTimeFormat.parse(stringValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return dateTime;
      case DECIMAL:
        return new BigDecimal(stringValue);
      case DOUBLE:
        return Double.valueOf(stringValue);
      case FLOAT:
        return Float.valueOf(stringValue);
      case INTEGER:
        return Integer.valueOf(stringValue);
      case LONG:
        return Long.valueOf(stringValue);
      case LIST:
      case MAP:
      case SHORT:
        return Short.valueOf(stringValue);
      default:
        return stringValue;
    }
  }
}
