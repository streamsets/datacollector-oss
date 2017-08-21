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
package com.streamsets.datacollector.rules;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DriftRuleEL {
  public static final String DRIFT_EL_PREFIX = "drift";

  private DriftRuleEL() {}

  abstract static class DriftDetector<T> {

    @VisibleForTesting
    Record getRecord() {
      return RecordEL.getRecordInContext();
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Map<String, Object> getContext() {
      return ((Map)ELEval.getVariablesInScope().getContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT));
    }

    /**
     * Return set of supported types for this drift detector.
     * Null denotes "I'm supporting all types, please don't do any checks".
     */
    public abstract Set<Field.Type> supportedTypes();

    @SuppressWarnings("unchecked")
    public boolean detect(String fieldPath, boolean ignoreWhenMissing) {
      boolean drifted = false;
      T valueInRecord = null;
      boolean missing = false;
      Field field = getRecord().get(fieldPath);
      if (field != null) {
        // Check that the field have supported type
        if(supportedTypes() != null && !supportedTypes().contains(field.getType())) {
          AlertInfoEL.setInfo(composeTypeAlert(fieldPath, field.getType(), supportedTypes()));
          return drifted;
        }

        if (field.getValue() != null) {
          valueInRecord = getValue(field);
        } else {
          missing = true;
        }
      } else {
        missing = true;
      }
      String key = getContextPrefix() + ":" + fieldPath;
      Map<String, Object> pipelineContext = getContext();
      boolean stored = pipelineContext.containsKey(key);
      T storedValue = (T) pipelineContext.get(key);
      if (missing) {
        if (!ignoreWhenMissing) {
          drifted = stored && storedValue != null;
        }
      } else {
        if (stored) {
          drifted = (storedValue == null && valueInRecord != null) || !storedValue.equals(valueInRecord);
        }
      }
      if (drifted) {
        pipelineContext.put(key, valueInRecord);
        AlertInfoEL.setInfo(composeAlert(fieldPath, storedValue, valueInRecord));
      }
      if (!stored) {
        if (ignoreWhenMissing) {
          if (!missing) {
            pipelineContext.put(key, valueInRecord);
          }
        } else {
          pipelineContext.put(key, valueInRecord);
        }
      }
      return drifted;
    }

    protected String getContextPrefix() {
      return (String) getContext().get(DataRuleEvaluator.RULE_ID_CONTEXT);
    }

    protected abstract T getValue(Field field);

    protected abstract String composeAlert(String fieldPath, T stored, T inRecord);

    String composeTypeAlert(String fieldPath, Field.Type givenType, Set<Field.Type> supportedTypes) {
      return Utils.format("Field {} have unsupported type of {}. Supported types are {}",
        fieldPath,
        givenType.toString(),
        StringUtils.join(supportedTypes, ",")
      );
    }
  }

  private static final DriftDetector<Integer> SIZE_DRIFT_DETECTOR = new DriftDetector<Integer>() {
    @Override
    public Set<Field.Type> supportedTypes() {
      return Sets.newHashSet(Field.Type.LIST, Field.Type.MAP, Field.Type.LIST_MAP);
    }

    @Override
    protected String getContextPrefix() {
      return super.getContextPrefix() + ":size";
    }

    @Override
    protected Integer getValue(Field field) {
      Integer size;
      switch (field.getType()) {
        case LIST:
          size = field.getValueAsList().size();
          break;
        case MAP:
          size = field.getValueAsMap().size();
          break;
        case LIST_MAP:
          size = field.getValueAsListMap().size();
          break;
        default:
          size = null;
          break;
      }
      return size;
    }

    @Override
    protected String composeAlert(String fieldPath, Integer stored, Integer inRecord) {
      return Utils.format(
          "The number of fields in field path '{}' changed from '{}' to '{}'.",
          fieldPath,
          stored,
          inRecord
      );
    }

  };

  @ElFunction(
      prefix = DRIFT_EL_PREFIX,
      name = "size",
      description = "Triggers an alert if the number of entries in the specified LIST, MAP or LIST_MAP field changes"
  )
  public static boolean size(
      @ElParam("fieldPath") String fieldPath,
      @ElParam("ignoreWhenMissing") boolean ignoreWhenMissing
  ) {
    return SIZE_DRIFT_DETECTOR.detect(fieldPath, ignoreWhenMissing);
  }

  private static final DriftDetector<Set<String>> NAME_DRIFT_DETECTOR = new DriftDetector<Set<String>>() {
    @Override
    public Set<Field.Type> supportedTypes() {
      return Sets.newHashSet(Field.Type.MAP, Field.Type.LIST_MAP);
    }

    @Override
    protected String getContextPrefix() {
      return super.getContextPrefix() + ":names";
    }

    @Override
    protected Set<String> getValue(Field field) {
      Set<String> names;
      switch (field.getType()) {
        case MAP:
        case LIST_MAP:
          names = new HashSet<>(field.getValueAsMap().keySet());
          break;
        default:
          names = null;
          break;
      }
      return names;
    }

    @Override
    protected String composeAlert(String fieldPath, Set<String> stored, Set<String> inRecord) {
      return Utils.format(
          "The field names for field path '{}' changed from '{}' to '{}'.",
          fieldPath,
          stored,
          inRecord
      );
    }

  };

  @ElFunction(
      prefix = DRIFT_EL_PREFIX,
      name = "names",
      description = "Triggers an alert if the keys of the entries in the specified MAP or LIST_MAP field changes"
  )
  @SuppressWarnings("unchecked")
  public static boolean names(
      @ElParam("fieldPath") String fieldPath,
      @ElParam("ignoreWhenMissing") boolean ignoreWhenMissing
  ) {
    return NAME_DRIFT_DETECTOR.detect(fieldPath, ignoreWhenMissing);
  }

  private static final DriftDetector<List<String>> ORDER_DRIFT_DETECTOR = new DriftDetector<List<String>>() {
    @Override
    public Set<Field.Type> supportedTypes() {
      return Sets.newHashSet(Field.Type.LIST_MAP);
    }

    @Override
    protected String getContextPrefix() {
      return super.getContextPrefix() + ":order";
    }

    @Override
    protected List<String> getValue(Field field) {
      List<String> names;
      switch (field.getType()) {
        case LIST_MAP:
          names = new ArrayList<>(field.getValueAsMap().keySet());
          break;
        default:
          names = null;
          break;
      }
      return names;
    }

    @Override
    protected String composeAlert(String fieldPath, List<String> stored, List<String> inRecord) {
      return Utils.format(
          "The order of fields in field path '{}' changed from '{}' to '{}'.",
          fieldPath,
          stored,
          inRecord
      );
    }

  };

  @ElFunction(
      prefix = DRIFT_EL_PREFIX,
      name = "order",
      description = "Triggers an alert if the order of the entries in the specified LIST_MAP field changes"
  )
  @SuppressWarnings("unchecked")
  public static boolean order(
      @ElParam("fieldPath") String fieldPath,
      @ElParam("ignoreWhenMissing") boolean ignoreWhenMissing
  ) {
    return ORDER_DRIFT_DETECTOR.detect(fieldPath, ignoreWhenMissing);
  }

  private static final DriftDetector<Field.Type> TYPE_DRIFT_DETECTOR = new DriftDetector<Field.Type>() {
    @Override
    public Set<Field.Type> supportedTypes() {
      return null;
    }

    @Override
    protected String getContextPrefix() {
      return super.getContextPrefix() + ":type";
    }

    @Override
    protected Field.Type getValue(Field field) {
      return field.getType();
    }

    @Override
    protected String composeAlert(String fieldPath, Field.Type stored, Field.Type inRecord) {
      return Utils.format("The data type for the '{}' field changed from '{}' to '{}'.", fieldPath, stored, inRecord);
    }

  };

  @ElFunction(
      prefix = DRIFT_EL_PREFIX,
      name = "type",
      description = "Triggers an alert if the type of the specified field changes"
  )
  public static boolean type(
      @ElParam("fieldPath") String fieldPath,
      @ElParam("ignoreWhenMissing") boolean ignoreWhenMissing
  ) {
    return TYPE_DRIFT_DETECTOR.detect(fieldPath, ignoreWhenMissing);
  }

}
