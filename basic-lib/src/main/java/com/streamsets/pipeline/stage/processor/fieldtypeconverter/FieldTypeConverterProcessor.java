/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DecimalScaleRoundingStrategy;
import com.streamsets.pipeline.lib.util.FieldPathExpressionUtil;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.LinkedList;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldTypeConverterProcessor.class);

  private final ConvertBy convertBy;
  private final List<FieldTypeConverterConfig> fieldTypeConverterConfigs;
  private final List<WholeTypeConverterConfig> wholeTypeConverterConfigs;
  private ELEval fieldPathEval;
  private ELVars fieldPathVars;

  public FieldTypeConverterProcessor(
      ConvertBy convertBy,
      List<FieldTypeConverterConfig> fieldTypeConverterConfigs,
      List<WholeTypeConverterConfig> wholeTypeConverterConfigs
  ) {
    this.convertBy = convertBy;
    this.fieldTypeConverterConfigs = fieldTypeConverterConfigs;
    this.wholeTypeConverterConfigs = wholeTypeConverterConfigs;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    fieldTypeConverterConfigs.forEach(config -> validate(config).ifPresent(issues::add));
    wholeTypeConverterConfigs.forEach(config -> validate(config).ifPresent(issues::add));

    fieldPathEval = getContext().createELEval("fields");
    fieldPathVars = getContext().createELVars();

    return issues;
  }

  private Optional<ConfigIssue> validate(BaseConverterConfig config) {
    if (config.targetType == Field.Type.ZONED_DATETIME) {
      ZonedDateTime now = ZonedDateTime.now();
      try {
        ZonedDateTime.parse(now.format(config.getFormatter()), config.getFormatter());
      } catch (DateTimeParseException ex) {
        return Optional.of(
            getContext().createConfigIssue(
                "TYPE_CONVERSION", "fieldTypeConverterConfigs", Errors.CONVERTER_03));
      }
    }
    return Optional.empty();
  }


  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
      switch (convertBy) {
        case BY_TYPE:
          Field rootField = record.get();
          if (rootField != null) {
            record.set(processByType("", rootField));
          }
          batchMaker.addRecord(record);
          break;
        case BY_FIELD:
          processByField(record, batchMaker);
          break;
        default:
          throw new IllegalArgumentException("Unknown convert by type: " + convertBy);
      }
  }

  private Field processByType(String matchingPath, Field rootField) throws StageException {
    switch (rootField.getType()) {
      case MAP:
      case LIST_MAP:
        if(rootField.getValue() == null) {
          return rootField;
        }
        for (Map.Entry<String, Field> entry : rootField.getValueAsMap().entrySet()) {
          entry.setValue(processByType(matchingPath + "/" + entry.getKey(), entry.getValue()));
        }
        break;
      case LIST:
        if(rootField.getValue() == null) {
          return rootField;
        }
        List<Field> fields = rootField.getValueAsList();
        for(int i = 0; i < fields.size(); i++) {
          fields.set(i, processByType(matchingPath + "[" + i + "]", fields.get(i)));
        }
        break;
      default:
        for(WholeTypeConverterConfig converterConfig : wholeTypeConverterConfigs) {
          if(converterConfig.sourceType == rootField.getType()) {
            rootField = convertField(matchingPath, rootField, converterConfig);
          }
        }
    }

    // Return original field
    return rootField;
  }

  private void processByField(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    final Set<String> fieldPaths = record.getEscapedFieldPaths();
    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        final List<String> matchingFieldPaths = new LinkedList<>(FieldPathExpressionUtil.evaluateMatchingFieldPaths(
            fieldToConvert,
            fieldPathEval,
            fieldPathVars,
            record,
            fieldPaths
        ));
        if (matchingFieldPaths.isEmpty()) {
          // FieldPathExpressionUtil.evaluateMatchingFieldPaths does NOT return the supplied param in its result
          // regardless, like FieldRegexUtil#getMatchingFieldPaths did, so we add manually here
          matchingFieldPaths.add(fieldToConvert);
        }
        for (String matchingField : matchingFieldPaths) {
          Field field = record.get(matchingField);
          if(field == null) {
            LOG.trace("Record does not have field {}. Ignoring conversion.", matchingField);
          } else {
            record.set(matchingField, convertField(matchingField, field, fieldTypeConverterConfig));
          }
        }
      }
    }
    batchMaker.addRecord(record);
  }

  private Field convertField(String matchingField, Field field, BaseConverterConfig converterConfig) throws StageException {
    if (field.getType() == Field.Type.STRING) {
      if (field.getValue() == null) {
        return Field.create(converterConfig.targetType, null);
      } else {
        try {
          String dateMask = null;
          if (converterConfig.targetType.isOneOf(Field.Type.DATE, Field.Type.DATETIME, Field.Type.TIME)) {
            dateMask = converterConfig.getDateMask();
          }
          return convertStringToTargetType(field,
              converterConfig.targetType,
              converterConfig.getLocale(),
              dateMask,
              converterConfig.scale,
              converterConfig.decimalScaleRoundingStrategy,
              converterConfig.getFormatter()
          );
        } catch (Exception e) {
          throw new OnRecordErrorException(Errors.CONVERTER_00,
              matchingField,
              field.getType(),
              field.getValueAsString(),
              converterConfig.targetType.name(),
              e
          );
        }
      }
    }

    if (converterConfig.targetType.isOneOf(Field.Type.STRING)
        && converterConfig.treatInputFieldAsDate && field.getType() == Field.Type.LONG) {
      if (field.getValue() == null) {
        return Field.create(converterConfig.targetType, null);
      } else {
        String dateMask = converterConfig.getDateMask();
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.create(converterConfig.targetType,
            dateFormat.format(field.getValueAsDatetime()));
      }
    }

    if (field.getType().isOneOf(Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME) && converterConfig.targetType.isOneOf(Field.Type.LONG, Field.Type.STRING, Field.Type.ZONED_DATETIME)) {
      if (field.getValue() == null) {
        return Field.create(converterConfig.targetType, null);
      } else if(converterConfig.targetType == Field.Type.LONG) {
        return Field.create(converterConfig.targetType, field.getValueAsDatetime().getTime());
      } else if(converterConfig.targetType == Field.Type.STRING) {
        String dateMask = converterConfig.getDateMask();
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.create(converterConfig.targetType, dateFormat.format(field.getValueAsDatetime()));
      } else if(converterConfig.targetType == Field.Type.ZONED_DATETIME) {
        return Field.create(Field.Type.ZONED_DATETIME, ZonedDateTime.ofInstant(
            field.getValueAsDatetime().toInstant(),
            ZoneId.of(converterConfig.zonedDateTimeTargetTimeZone)
        ));
      }
    }

    if (field.getType() == Field.Type.ZONED_DATETIME) {
      switch (converterConfig.targetType) {
        case STRING:
          if(field.getValue() == null) {
            return Field.create(Field.Type.STRING, null);
          } else {
            return Field.create(converterConfig.getFormatter().format(field.getValueAsZonedDateTime()));
          }
        case ZONED_DATETIME:
          return field;
        default:
          throw new OnRecordErrorException(Errors.CONVERTER_04, converterConfig.targetType);
      }
    }

    if(field.getType() == Field.Type.BYTE_ARRAY && converterConfig.targetType == Field.Type.STRING) {
      try {
        if(field.getValue() == null) {
          return Field.create(converterConfig.targetType, null);
        }

        return Field.create(converterConfig.targetType, new String(field.getValueAsByteArray(), converterConfig.encoding));
      } catch (Exception e) {
        throw new OnRecordErrorException(Errors.CONVERTER_00, matchingField, field.getType(), field.getValue(), converterConfig.targetType, e);
      }
    }

    if (converterConfig.targetType == Field.Type.DECIMAL &&
        field.getType().isOneOf(
            Field.Type.BYTE,
            Field.Type.SHORT,
            Field.Type.INTEGER,
            Field.Type.FLOAT,
            Field.Type.LONG,
            Field.Type.DOUBLE,
            Field.Type.DECIMAL
        )) {
      try {
        if(field.getValue() == null) {
          return Field.create(converterConfig.targetType, null);
        }

        Field changedField = Field.create(converterConfig.targetType, field.getValue());
        BigDecimal newValue = adjustScaleIfNeededForDecimalConversion(
            changedField.getValueAsDecimal(),
            converterConfig.scale,
            converterConfig.decimalScaleRoundingStrategy
        );
        return Field.create(newValue);
      } catch (Exception e) {
        throw new OnRecordErrorException(Errors.CONVERTER_00, matchingField, field.getType(), field.getValue(), converterConfig.targetType, e);
      }
    }

    if(field.getType().isOneOf(Field.Type.BOOLEAN) && converterConfig.targetType.isOneOf(Field.Type.INTEGER, Field.Type.SHORT, Field.Type.LONG)) {
      if(field.getValue() == null) {
        return Field.create(converterConfig.targetType, null);
      }

      return Field.create(converterConfig.targetType, field.getValueAsBoolean() ? 1 : 0);
    }

    // Use the built in type conversion provided by TypeSupport
    try {
      // Use the built in type conversion provided by TypeSupport
      return Field.create(converterConfig.targetType, field.getValue());
    } catch (IllegalArgumentException e) {
      throw new OnRecordErrorException(Errors.CONVERTER_00,
          matchingField,
          field.getType(),
          field.getValue(),
          converterConfig.targetType.name()
      );
    }
  }

  private BigDecimal adjustScaleIfNeededForDecimalConversion(BigDecimal value, int scale, DecimalScaleRoundingStrategy roundingStrategy) {
    return (scale != -1)? value.setScale(scale, roundingStrategy.getRoundingStrategy()) : value;
  }

  private Field convertStringToTargetType(
      Field field,
      Field.Type targetType,
      Locale dataLocale,
      String dateMask,
      int scale,
      DecimalScaleRoundingStrategy decimalScaleRoundingStrategy,
      DateTimeFormatter dateTimeFormatter
  ) throws ParseException {
    String stringValue = field.getValueAsString();
    switch(targetType) {
      case BOOLEAN:
        return Field.create(Field.Type.BOOLEAN, Boolean.valueOf(stringValue.trim()));
      case BYTE:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).byteValue());
      case BYTE_ARRAY:
        return Field.create(stringValue.getBytes(StandardCharsets.UTF_8));
      case CHAR:
        return Field.create(stringValue.charAt(0));
      case DATE:
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDate(dateFormat.parse(stringValue.trim()));
      case DATETIME:
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDatetime(dateTimeFormat.parse(stringValue.trim()));
      case TIME:
        java.text.DateFormat timeFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createTime(timeFormat.parse(stringValue.trim()));
      case ZONED_DATETIME:
        return Field.createZonedDateTime(ZonedDateTime.parse(stringValue.trim(), dateTimeFormatter));
      case DECIMAL:
        NumberFormat decimalFormat = NumberFormat.getInstance(dataLocale);
        DecimalFormat df = (DecimalFormat) decimalFormat;
        df.setParseBigDecimal(true);
        Number decimal = df.parse(stringValue.trim());
        BigDecimal bigDecimal = adjustScaleIfNeededForDecimalConversion(new BigDecimal(decimal.toString()), scale, decimalScaleRoundingStrategy);
        Field decimalField = Field.create(Field.Type.DECIMAL, bigDecimal);
        decimalField.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, String.valueOf(bigDecimal.precision()));
        decimalField.setAttribute(HeaderAttributeConstants.ATTR_SCALE, String.valueOf(bigDecimal.scale()));
        return decimalField;
      case DOUBLE:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue.trim()).doubleValue());
      case FLOAT:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue.trim()).floatValue());
      case INTEGER:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue.trim()).intValue());
      case LONG:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue.trim()).longValue());
      case SHORT:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue.trim()).shortValue());
      case FILE_REF:
        throw new IllegalArgumentException(Utils.format("Cannot convert String value to type {}", targetType));
      default:
        return field;
    }
  }

}
