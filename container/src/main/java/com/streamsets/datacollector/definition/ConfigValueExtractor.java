/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ConfigValueExtractor {
  private static final ConfigValueExtractor EXTRACTOR = new ConfigValueExtractor() {};

  public static ConfigValueExtractor get() {
    return EXTRACTOR;
  }

  public final static Set<Class> BOOLEAN_TYPES = ImmutableSet.<Class>of(Boolean.class, Boolean.TYPE);
  public final static Set<Class> NUMBER_TYPES = ImmutableSet.<Class>of(Byte.class, Byte.TYPE,
                                                                   Short.class, Short.TYPE,
                                                                   Integer.class, Integer.TYPE,
                                                                   Long.class, Long.TYPE,
                                                                   Float.class, Float.TYPE,
                                                                   Double.class, Double.TYPE);
  public final static Set<Class> CHARACTER_TYPES = ImmutableSet.<Class>of(Character.class, Character.TYPE);

  @SuppressWarnings("unchecked")
  public List<ErrorMessage> validate(Field field, ConfigDef.Type type, String valueStr, Object contextMsg,
      boolean isTrigger) {
    List<ErrorMessage> errors = new ArrayList<>();
    if (!valueStr.isEmpty()) {
      if (ElUtil.isElString(valueStr)) {
        if (isTrigger) {
          errors.add(new ErrorMessage(DefinitionError.DEF_000, contextMsg, valueStr));
        }
      } else {
        switch (type) {
          case BOOLEAN:
            if (!BOOLEAN_TYPES.contains(field.getType())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_001, contextMsg, field.getType()));
            }
            break;
          case NUMBER:
            if (!NUMBER_TYPES.contains(field.getType())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_002, contextMsg));
            }
            break;
          case STRING:
          case MODEL:
            if (!String.class.isAssignableFrom(field.getType()) && !field.getType().isEnum()) {
              errors.add(new ErrorMessage(DefinitionError.DEF_003, contextMsg, field.getType()));
            }
            if (field.getType().isEnum()) {
              try {
                Enum.valueOf(((Class<Enum>) field.getType()), valueStr);
              } catch (IllegalArgumentException ex) {
                errors.add(new ErrorMessage(DefinitionError.DEF_004, contextMsg, field.getType(), valueStr));
              }
            }
            break;
          case LIST:
            if (!List.class.isAssignableFrom(field.getType())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_005, contextMsg, field.getType()));
            }
            try {
              ObjectMapperFactory.get().readValue(valueStr, List.class);
            } catch (Exception ex) {
              errors.add(new ErrorMessage(DefinitionError.DEF_006, contextMsg, valueStr));
            }
            break;
          case MAP:
            if (!Map.class.isAssignableFrom(field.getType())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_007, contextMsg, field.getType()));
            }
            try {
              ObjectMapperFactory.get().readValue(valueStr, LinkedHashMap.class);
            } catch (Exception ex) {
              errors.add(new ErrorMessage(DefinitionError.DEF_008, contextMsg, valueStr));
            }
            break;
          case CHARACTER:
            if (!CHARACTER_TYPES.contains(field.getType())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_009, contextMsg));
            }
            if (valueStr.length() > 1) {
              errors.add(new ErrorMessage(DefinitionError.DEF_010, contextMsg, valueStr));
            }
            break;
          case TEXT:
            if (!String.class.isAssignableFrom(field.getType())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_011, contextMsg, field.getType()));
            }
            break;
        }
      }
    }
    return errors;
  }

  public List<ErrorMessage> validate(Field field, ConfigDef def, Object contextMsg) {
    return validate(field, def.type(), def.defaultValue(), contextMsg, false);
  }

  public Object extract(Field field, ConfigDef def, Object contextMsg) {
    return extract(field, def.type(), def.defaultValue(), contextMsg, false);
  }

  @SuppressWarnings("unchecked")
  public Object extract(Field field, ConfigDef.Type type, String valueStr, Object contextMsg, boolean isTrigger) {
    List<ErrorMessage> errors = validate(field, type, valueStr, contextMsg, isTrigger);
    if (errors.isEmpty()) {
      Object value = null;
      if (valueStr == null || valueStr.isEmpty()) {
        value = type.getDefault();
      } else {
        if (ElUtil.isElString(valueStr)) {
          value = valueStr;
        } else {
          try {
            switch (type) {
              case BOOLEAN:
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
                }
                break;
              case STRING:
              case MODEL:
                value = (field.getType() == String.class) ? valueStr
                                                          : Enum.valueOf(((Class<Enum>)field.getType()), valueStr);
                break;
              case LIST:
                value = ObjectMapperFactory.get().readValue(valueStr, List.class);
                break;
              case MAP:
                Map<String, ?> map = ObjectMapperFactory.get().readValue(valueStr, LinkedHashMap.class);
                List list = new ArrayList();
                for (Map.Entry<String, ?> entry : map.entrySet()) {
                  list.add(ImmutableMap.of("key", entry.getKey(), "value", entry.getValue()));
                }
                value = list;
                break;
              case CHARACTER:
                value = valueStr.charAt(0);
                break;
              case TEXT:
                value = valueStr;
                break;
            }
          } catch (Exception ex) {
            throw new RuntimeException(Utils.format("It should not happen: {}", ex.getMessage()), ex);
          }
        }
      }
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid configuration value: {}", errors));
    }
  }

}
