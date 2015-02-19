/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Field Masker",
    description = "Masks field values",
    icon="mask.png"
)
@ConfigGroups(com.streamsets.pipeline.stage.processor.fieldmask.ConfigGroups.class)
public class FieldMaskProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldMaskProcessor.class);

  private static final String FIXED_LENGTH_MASK = "xxxxxxxxxx";
  private static final char NON_MASK_CHAR = '#';
  private static final char MASK_CHAR = 'x';

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "",
      description = "",
      displayPosition = 10,
      group = "MASKING"
  )
  @ComplexField
  public List<FieldMaskConfig> fieldMaskConfigs;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    for(FieldMaskConfig fieldMaskConfig : fieldMaskConfigs) {
      for (String toMask : fieldMaskConfig.fields) {
        if(record.has(toMask)) {
          Field field = record.get(toMask);
          if (field.getType() != Field.Type.STRING) {
            LOG.info("The field {} in record {} is of type {}. Ignoring field.", toMask,
              record.getHeader().getSourceId(), field.getType().name());
          } else if (field.getValue() == null) {
            LOG.info("The field {} in record {} has null value. Ignoring field.", toMask,
              record.getHeader().getSourceId());
          } else {
            LOG.debug("Applying mask '{}' to field {} in record {}.", fieldMaskConfig.maskType, toMask,
              record.getHeader().getSourceId());
            Field newField = Field.create(maskField(field, fieldMaskConfig));
            record.set(toMask, newField);
          }
        } else {
          LOG.info("Could not find field {} in record {}.", toMask, record.getHeader().getSourceId());
        }
      }
    }
    batchMaker.addRecord(record);
  }

  private String maskField(Field field, FieldMaskConfig fieldMaskConfig) {
    if(fieldMaskConfig.maskType == MaskType.FIXED_LENGTH) {
      return fixedLengthMask();
    } else if (fieldMaskConfig.maskType == MaskType.VARIABLE_LENGTH) {
      return variableLengthMask(field.getValueAsString());
    } else if (fieldMaskConfig.maskType == MaskType.CUSTOM) {
      return mask(field.getValueAsString(), fieldMaskConfig.mask);
    }
    //Should not happen
    return null;
  }

  @VisibleForTesting
  String mask(String toMask, String mask) {
    int index = 0;
    StringBuilder masked = new StringBuilder();
    for (int i = 0; i < mask.length() && index < toMask.length(); i++) {
      char c = mask.charAt(i);
      if (c == NON_MASK_CHAR) {
        masked.append(toMask.charAt(index));
        index++;
      } else if (c == MASK_CHAR) {
        masked.append(c);
        index++;
      } else {
        masked.append(c);
        //The data can be either formatted or not
        //for example ssn data could be 123456789 or 123-45-6789
        if(toMask.charAt(index) == c) {
          //the data is already in the required format
          index++;
        }
      }
    }
    return masked.toString();
  }

  @VisibleForTesting
  String fixedLengthMask() {
    return FIXED_LENGTH_MASK;
  }

  @VisibleForTesting
  String variableLengthMask(String toMask) {
    StringBuilder masked = new StringBuilder();
    for (int i = 0; i < toMask.length(); i++) {
      masked.append(MASK_CHAR);
    }
    return masked.toString();
  }
}
