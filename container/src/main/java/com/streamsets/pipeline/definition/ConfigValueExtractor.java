/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.util.ElUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class ConfigValueExtractor {
  private static final ConfigValueExtractor EXTRACTOR = new ConfigValueExtractor() {};

  public static ConfigValueExtractor get() {
    return EXTRACTOR;
  }

  public Object extract(Field field, ConfigDef def, Object contextMsg) {
    return extract(field, def.type(), def.defaultValue(), contextMsg, false);
  }

  @SuppressWarnings("unchecked")
  public Object extract(Field field, ConfigDef.Type type, String valueStr, Object contextMsg, boolean isTrigger) {
    Object value = null;
    if (valueStr == null || valueStr.isEmpty()) {
      value = type.getDefault();
    } else {
      if (ElUtil.isElString(valueStr)) {
        Utils.checkArgument(!isTrigger, Utils.formatL("{}, trigger values cannot have EL expressions '{}'", contextMsg,
                                                      valueStr));
        value = valueStr;
      } else {
        try {
          switch (type) {
            case BOOLEAN:
              Utils.checkArgument(field.getType() == Boolean.TYPE || field.getType() == Boolean.class,
                                  Utils.formatL("{}, configuration field is '{}', it must be 'boolean' or 'Boolean'",
                                                contextMsg, field.getType()));
              value = Boolean.parseBoolean(valueStr);
              break;
            case NUMBER:
              if (field.getType() == Byte.TYPE || field.getType() == Byte.class) {
                value = Byte.parseByte(valueStr);
              } else  if (field.getType() == Short.TYPE || field.getType() == Short.class) {
                value = Short.parseShort(valueStr);
              } else   if (field.getType() == Integer.TYPE || field.getType() == Integer.class) {
                value = Integer.parseInt(valueStr);
              } else if (field.getType() == Long.TYPE || field.getType() == Long.class) {
                value = Long.parseLong(valueStr);
              } else if (field.getType() == Float.TYPE || field.getType() == Float.class) {
                value = Float.parseFloat(valueStr);
              } else if (field.getType() == Double.TYPE || field.getType() == Double.class) {
                value = Double.parseDouble(valueStr);
              } else {
                throw new IllegalArgumentException(Utils.format("{}, configuration field is not numeric", contextMsg));
              }
              break;
            case STRING:
              Utils.checkArgument(field.getType() ==  String.class || field.getType().isEnum(), Utils.formatL(
                  "{}, configuration field is '{}', it must be 'String'", contextMsg, field.getType()));
              value = (field.getType() == String.class) ? valueStr
                                                        : Enum.valueOf(((Class<Enum>)field.getType()), valueStr);
              break;
            case LIST:
              Utils.checkArgument(List.class.isAssignableFrom(field.getType()), Utils.formatL(
                  "{}, configuration field is '{}', it must be 'java.lang.List'", contextMsg, field.getType()));
              value = ObjectMapperFactory.get().readValue(valueStr, List.class);
              break;
            case MAP:
              Utils.checkArgument(Map.class.isAssignableFrom(field.getType()), Utils.formatL(
                  "{}, configuration field is '{}', it must be 'java.lang.Map'", contextMsg, field.getType()));
              Map<String, ?> map = ObjectMapperFactory.get().readValue(valueStr, LinkedHashMap.class);
              List list = new ArrayList();
              for (Map.Entry<String, ?> entry : map.entrySet()) {
                list.add(ImmutableMap.of("key", entry.getKey(), "value", entry.getValue()));
              }
              value = list;
              break;
            case MODEL:
              Utils.checkArgument(valueStr.isEmpty(), Utils.formatL(
                  "{}, configuration field is a 'Model', cannot have a default value", contextMsg));
              value = null;
              break;
            case CHARACTER:
              Utils.checkArgument(field.getType() == Character.TYPE || field.getType() == Character.class,
                                  Utils.formatL("{}, configuration field is '{}', it must be 'char' or 'Character'",
                                                contextMsg, field.getType()));
              if (valueStr.length() == 1) {
                value = valueStr.charAt(0);
              } else {
                throw new IllegalArgumentException(Utils.format("{}, the value '{}' is not a character", contextMsg,
                                                                valueStr));
              }
              break;
            case TEXT:
              Utils.checkArgument(field.getType() ==  String.class, Utils.formatL(
                  "{}, configuration field is '{}', it must be 'String'", contextMsg, field.getType()));
              value = valueStr;
              break;
          }
        } catch (Exception ex) {
          throw new IllegalArgumentException(Utils.format("{}, value '{}' cannot be parsed as '{}': {}",
                                                          contextMsg, valueStr, type, ex.getMessage()), ex);
        }
      }
    }
    return value;
  }

}
