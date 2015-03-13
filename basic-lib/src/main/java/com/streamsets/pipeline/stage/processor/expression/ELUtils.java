/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ELUtils {

  private static final String FIELD_EL_PREFIX = "field";
  private static final String TRANSFORM_EL_PREFIX = "transform";

  public static interface Transformer {
    public void init(Field originalField);
    public void processElement(Object element);
    public Object done();
  }

  public static abstract class TransformerToMap implements Transformer {
    private Map<String, Field> map;

    @Override
    public void init(Field originalField) {
      map = new LinkedHashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(Object element) {
      if (element instanceof  Map.Entry) {
        processMapEntry((Map.Entry<String, Field>) element);
      } else {
        processField((Field) element);
      }
    }

    protected void put(String key, Field value) {
      map.put(key, value);
    }

    protected void processMapEntry(Map.Entry<String, Field> entry) {
      throw new UnsupportedOperationException();
    }

    protected void processField(Field field) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object done() {
      return map;
    }

  }

  @ElFunction(
      prefix = FIELD_EL_PREFIX,
      name = "scan",
      description = "Scans the fieldPath value applying a toMap transformation.")
  public static Object scan(
      @ElParam("fieldPath") String fieldPath,
      @ElParam("transformation") Transformer transformer
  ) {
    Record record = RecordEL.getRecordInContext();
    Object result = null;
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        transformer.init(field);
        if (field.getValue() != null) {
          switch (field.getType()) {
            case LIST:
              for (Object e : field.getValueAsList()) {
                transformer.processElement(e);
              }
              break;
            case MAP:
              for (Object e : field.getValueAsMap().entrySet()) {
                transformer.processElement(e);
              }
              break;
            default:
              transformer.processElement(field);
              break;
          }
        }
        result = transformer.done();
      }
    }
    return result;
  }

  @ElFunction(
      prefix = TRANSFORM_EL_PREFIX,
      name = "insertToMap",
      description = "Insert key/value of Map elements into a single Map per scan")
  public static TransformerToMap insertToMap(
      @ElParam("keyValue") final String keyElement,
      @ElParam("valueValue") final String valueElement
  ) {
    return new TransformerToMap() {
      @Override
      protected void processField(Field field) {
        if (field != null && field.getType() == Field.Type.MAP) {
          Map<String, Field> map = field.getValueAsMap();
          Field keyF = map.get(keyElement);
          if (keyF != null && keyF.getValue() != null) {
            Field keyV = map.get(valueElement);
            if (keyV != null) {
              put(keyF.getValueAsString(), keyV);
            }
          }
        }
      }
    };
  }

  @ElFunction(
      prefix = TRANSFORM_EL_PREFIX,
      name = "renameIfMatches",
      description = "Finds the first string value matching the regExPattern in a MAP and renames its key")
  public static Transformer detectValuePattern(
      @ElParam("regExPattern") final String regExPattern,
      @ElParam("newName") final String newName
  ) {
    return new Transformer() {
      private Field root;
      private Pattern pattern;
      private String foundKey;
      private Field foundValue;

      @Override
      public void init(Field field) {
        this.root = field;
        pattern = Pattern.compile(regExPattern);
      }

      @Override
      @SuppressWarnings("unchecked")
      public void processElement(Object element) {
        if (element instanceof Map.Entry) {
          Map.Entry<String, Field> entry = (Map.Entry<String, Field>) element;
          Field field = entry.getValue();
          if (field != null && field.getValue() != null && field.getType() == Field.Type.STRING) {
            if (pattern.matcher(field.getValueAsString()).matches()) {
              foundKey = entry.getKey();
              foundValue = field;
            }
          }
        }
      }

      @Override
      public Object done() {
        if (foundValue != null) {
          root.getValueAsMap().put(newName, foundValue);
          root.getValueAsMap().remove(foundKey);
        }
        return root.getValue();
      }
    };

  }

}
