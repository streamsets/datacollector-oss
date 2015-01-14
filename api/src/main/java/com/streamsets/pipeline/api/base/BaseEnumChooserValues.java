/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseEnumChooserValues implements ChooserValues {

  public interface EnumWithLabel {
    public String getLabel();
  }

  private static final Map<Class<? extends Enum>, List<String>> ENUM_VALUES_MAP = new ConcurrentHashMap<>();
  private static final Map<Class<? extends Enum>, List<String>> ENUM_LABELS_MAP = new ConcurrentHashMap<>();

  private List<String> values;
  private List<String> labels;

  protected BaseEnumChooserValues(Class<? extends Enum> klass) {
    Utils.checkNotNull(klass, "klass");
    if (!ENUM_LABELS_MAP.containsKey(klass)) {
      synchronized (BaseEnumChooserValues.class) {
        if (!ENUM_LABELS_MAP.containsKey(klass)) {
          boolean isEnumWithLabels = EnumWithLabel.class.isAssignableFrom(klass);
          List<String> values = new ArrayList<>();
          List<String> labels = new ArrayList<>();
          for (Enum e : klass.getEnumConstants()) {
            values.add(e.toString());
            if (isEnumWithLabels) {
              labels.add(((EnumWithLabel) e).getLabel());
            } else {
              labels.add(e.toString());
            }
          }
          ENUM_VALUES_MAP.put(klass, Collections.unmodifiableList(values));
          ENUM_LABELS_MAP.put(klass, Collections.unmodifiableList(labels));
        }
      }
    }
    values = ENUM_VALUES_MAP.get(klass);
    labels = ENUM_LABELS_MAP.get(klass);
  }

  @Override
  public List<String> getValues() {
    return values;
  }

  @Override
  public List<String> getLabels() {
    return labels;
  }
}
