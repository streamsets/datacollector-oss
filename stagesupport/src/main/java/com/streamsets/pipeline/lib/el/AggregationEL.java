/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.FieldUtils;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AggregationEL {
  private static final String FIELDS_TO_PREVIOUS_PATHS_CONTEXT_VAR = "fieldsToPreviousPaths";

  @ElFunction(
      name = "sum",
      description = "Sums the values in the given list of fields."
  )
  public static Field sum(@ElParam("input") List<Field> inputs) {
    final NumericResultUpdater sumUpdater = new NumericResultUpdater() {
      @Override
      public long updateLongResult(long currentValue, long newInput) {
        return currentValue + newInput;
      }

      @Override
      public double updateDoubleResult(double currentValue, double newInput) {
        return currentValue + newInput;
      }

      @Override
      public BigDecimal updateDecimalResult(BigDecimal currentValue, BigDecimal newInput) {
        return currentValue.add(newInput == null ? new BigDecimal(0) : newInput);
      }
    };

    return numericAggregationHelper(
        inputs,
        sumUpdater,
        0l,
        0.0d,
        new BigDecimal(0)
    );
  }

  @ElFunction(
      name = "min",
      description = "Returns the minimum value amongst the given list of fields."
  )
  public static Field min(@ElParam("input") List<Field> inputs) {
    return numericAggregationHelper(
        inputs,
        new MinMaxResultUpdater(false),
        Long.MAX_VALUE,
        Double.MAX_VALUE,
        null
    );
  }

  @ElFunction(
      name = "max",
      description = "Returns the minimum value amongst the given list of fields."
  )
  public static Field max(@ElParam("input") List<Field> inputs) {
    return numericAggregationHelper(
        inputs,
        new MinMaxResultUpdater(true),
        Long.MIN_VALUE,
        Double.MIN_VALUE,
        null);
  }

  public static Field numericAggregationHelper(
      @ElParam("input") List<Field> inputs,
      NumericResultUpdater updater,
      long initialLongValue,
      double initialDoubleValue,
      BigDecimal initialDecimalValue
  ) {
    if (inputs == null || inputs.isEmpty()) {
      // TODO: something better here?
      return Field.create(0l);
    }
    Field.Type type = null;
    long integralResult = initialLongValue;
    double floatingPointResult = initialDoubleValue;
    BigDecimal decimalResult = initialDecimalValue;
    for (Field input : inputs) {
      if (type == null) {
        type = input.getType();
      } else if (type != input.getType()) {
        throw new IllegalStateException(String.format(
            "Mixed types were detected in input list; encountered %s with value %s, previously saw %s",
            input.getType().name(),
            input.getValue(),
            type.name()
        ));
      }
      switch (type) {
        case INTEGER:
          integralResult = updater.updateLongResult(integralResult, input.getValueAsInteger());
          break;
        case LONG:
          integralResult = updater.updateLongResult(integralResult, input.getValueAsLong());
          break;
        case FLOAT:
          floatingPointResult = updater.updateDoubleResult(floatingPointResult, input.getValueAsFloat());
          break;
        case DOUBLE:
          floatingPointResult = updater.updateDoubleResult(floatingPointResult, input.getValueAsDouble());
          break;
        case DECIMAL:
          decimalResult = updater.updateDecimalResult(decimalResult, input.getValueAsDecimal());
          break;
        default:
          throw new IllegalStateException(String.format(
              "Unsupported type detected in input list; encountered %s with value %s, which is not summable",
              input.getType().name(),
              input.getValue()
          ));
      }
    }

    switch (type) {
      case INTEGER:
      case LONG:
        return Field.create(integralResult);
      case FLOAT:
      case DOUBLE:
        return Field.create(floatingPointResult);
      case DECIMAL:
        return Field.create(decimalResult);
      default:
        throw new IllegalStateException(String.format(
            "Should not reach here (unsupported type for return: %s",
            type.name()
        ));
    }
  }

  interface NumericResultUpdater {
    long updateLongResult(long currentValue, long newInput);
    double updateDoubleResult(double currentValue, double newInput);
    BigDecimal updateDecimalResult(BigDecimal currentValue, BigDecimal newInput);
  }

  static class MinMaxResultUpdater implements NumericResultUpdater {
    private final boolean max;

    MinMaxResultUpdater(boolean max) {
      this.max = max;
    }

    private <T extends Number> T minMaxHelper(T currentValue, T newInput, boolean newLarger) {
      if (currentValue == null) {
        return newInput;
      }
      if (max) {
        return newLarger ? newInput : currentValue;
      } else {
        return newLarger ? currentValue : newInput;
      }
    }

    @Override
    public long updateLongResult(long currentValue, long newInput) {
      return minMaxHelper(currentValue, newInput, newInput > currentValue);
    }

    @Override
    public double updateDoubleResult(double currentValue, double newInput) {
      return minMaxHelper(currentValue, newInput, newInput > currentValue);
    }

    @Override
    public BigDecimal updateDecimalResult(BigDecimal currentValue, BigDecimal newInput) {
      return minMaxHelper(
          currentValue,
          newInput,
          currentValue == null || (newInput != null && newInput.compareTo(currentValue) >= 1)
      );
    }
  }

  @ElFunction(
      name = "asFields",
      description = "Maps a list of values to the equivalent list of fields."
  )
  public static List<Object> asFields(@ElParam("values") List<Object> values) {
    if (values == null || values.isEmpty()) {
      return new LinkedList<>();
    }
    return values.stream().filter(value -> value != null).map(value -> {
      final Field.Type fieldType = FieldUtils.getTypeFromObject(value);
      return Field.create(fieldType, value);
    }).collect(Collectors.toList());
  }

  @ElFunction(
      name = "map",
      description = "Maps a function to each element of the input list."
  )
  public static List<Object> map(
      @ElParam("inputs") List<Field> inputs,
      @ElParam("mapFunction") Function<Field, ?> function
  ) {
    if (inputs == null || inputs.isEmpty()) {
      return new LinkedList<>();
    }
    return inputs.stream().map(field -> function.apply(field)).collect(Collectors.toList());
  }

  @ElFunction(
      name = "previousPath",
      description = "Returns the previous field path, before its path was changed via the mapper."
  )
  public static Function<Field, String> previousFieldPath() {
    return (field -> {
      final Map<Field, String> fieldsToPreviousPaths = getFieldsToPreviousPathsInContext();
      return fieldsToPreviousPaths.get(field);
    });
  }

  @ElFunction(
      name = "fieldByPreviousPath",
      description = "For a mapped field, returns a new field of type MAP whose key is the previous path, and the" +
          " value is the field itself. "
  )
  // TODO: figure out how to get this to work
  // we seem to not like forward slashes in the field name and can't figure out how to escape them
  public static Function<Field, Field> previousFieldPathToField() {
    return (field -> {
      final Map<Field, String> fieldsToPreviousPaths = getFieldsToPreviousPathsInContext();
      final String previousPath = fieldsToPreviousPaths.get(field);
      if (previousPath == null) {
        throw new IllegalStateException(String.format(
            "Previous path for field %s not found; all previous paths: %s",
            field,
            fieldsToPreviousPaths
        ));
      }
      return Field.create(Collections.singletonMap(previousPath, field));
    });
  }

  static Map<Field, String> getFieldsToPreviousPathsInContext() {
    return (Map<Field, String>) ELEval.getVariablesInScope().getContextVariable(FIELDS_TO_PREVIOUS_PATHS_CONTEXT_VAR);
  }

  public static void setFieldsToPreviousPathsInContext(ELVars variables, Map<Field, String> fieldsToPreviousPaths) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(FIELDS_TO_PREVIOUS_PATHS_CONTEXT_VAR, fieldsToPreviousPaths);
  }
}
