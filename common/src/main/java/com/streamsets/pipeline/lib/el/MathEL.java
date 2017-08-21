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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * Encapsulated methods available in java.lang.Math
 */
public class MathEL {

  // Various types that the Math class supports
  enum GivenType {
    DOUBLE,
    FLOAT,
    INT,
    LONG,
  }

  /**
   * Abstract operation that encapsulates single operation like abs, just executed for all supported data types. We're
   * not converting the incoming argument to a single shared type as we want to retain the type information. E.g. if
   * user called math:abs(int) result will be int, if it was called as math:abs(double) the result will be double. The
   * only exception is a String input that is converted to a number in all cases (either to double if it contains dot
   * or to long if not). Any advanced smartness will require user to cast their types properly.
   */
  private static abstract class Operation {
    String operationName;
    public Operation(String name) {
      this.operationName = name;
    }

    public Object process(Object ...params) {
      if(params == null || params.length < 1 || params.length > 2) {
        throw new IllegalArgumentException(Utils.format("Invalid number of arguments for {}", operationName));
      }

      // There always have to be at least one argument
      Object param = params[0];
      if(param == null) {
        return null;
      }
      if(param instanceof String) {
        param = convertStringToAppropriateNumber((String) param);
      }

      // Second argument might be missing
      Object secondParam = null;
      if(params.length > 1) {
        secondParam = params[1];
        if(secondParam == null) {
          return null;
        }
        if(secondParam instanceof String) {
          secondParam = convertStringToAppropriateNumber((String) secondParam);
        }
        if(param.getClass() != secondParam.getClass())  {
          throw new IllegalArgumentException(Utils.format("Arguments for operation {} doesn't have the same type.", operationName));
        }
      }

      // Detect type, so that parent classes don't have to do that
      GivenType type;
      if (param.getClass() == Double.class) {
        type = GivenType.DOUBLE;
      } else if (param.getClass() == Float.class) {
        type = GivenType.FLOAT;
      } else if (param.getClass() == Integer.class) {
        type = GivenType.INT;
      } else if (param.getClass() == Long.class) {
        type = GivenType.LONG;
      } else {
        throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }

      return calculate(type, param, secondParam);
    }

    protected abstract Object calculate(GivenType type, Object param, Object secondParam);
  }

  /**
   * We need to support Strings as some information that user might need to deal with is inherently stored in String
   * variables - for example header values or CSV files.
   *
   * The way we're processing strings is - if it contains dot, then it's assumed to be a double otherwise long. If we for
   * whatever reason fail to parse the number and error is thrown.
   */
  private static Object convertStringToAppropriateNumber(String value) {
    if(value.contains(".")) {
      return Double.valueOf(value);
    } else {
      return Long.valueOf(value);
    }
  }

  /*
   * Math:abs
   */
  private static Operation ABS = new Operation("abs") {
    @Override
    protected Object calculate(GivenType type, Object param, Object secondParam) {
      switch (type) {
        case DOUBLE:  return Math.abs((double)param);
        case FLOAT:   return Math.abs((float)param);
        case INT:     return Math.abs((int)param);
        case LONG:    return Math.abs((long)param);
        default:
          throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }
    }
  };
  @ElFunction(
    prefix = "math",
    name = "abs",
    description = "Returns absolute value of the argument."
  )
  public static Object abs(@ElParam("number") Object number) {
      return ABS.process(number);
  }

  /*
   * Math:ceil
   */
  private static Operation CEIL = new Operation("ceil") {
    @Override
    protected Object calculate(GivenType type, Object param, Object secondParam) {
      switch (type) {
        case DOUBLE:
        case FLOAT:
          return Math.ceil((double)param);
        default:
          throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }
    }
  };
  @ElFunction(
    prefix = "math",
    name = "ceil",
    description = "Returns the smallest (closest to negative infinity) double value that is greater than or equal to the argument and is equal to a mathematical integer."
  )
  public static Object ceil(@ElParam("number") Object number) {
      return CEIL.process(number);
  }

  /*
   * Math:floor
   */
  private static Operation FLOOR = new Operation("floor") {
    @Override
    protected Object calculate(GivenType type, Object param, Object secondParam) {
      switch (type) {
        case DOUBLE:
        case FLOAT:
          return Math.floor((double)param);
        default:
          throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }
    }
  };
  @ElFunction(
    prefix = "math",
    name = "floor",
    description = "Returns the largest (closest to positive infinity) double value that is less than or equal to the argument and is equal to a mathematical integer."
  )
  public static Object floor(@ElParam("number") Object number) {
      return FLOOR.process(number);
  }

  /*
   * Math:max
   */
  private static Operation MAX = new Operation("max") {
    @Override
    protected Object calculate(GivenType type, Object param, Object secondParam) {
      switch (type) {
        case DOUBLE:  return Math.max((double)param, (double)secondParam);
        case FLOAT:   return Math.max((float)param, (double)secondParam);
        case INT:     return Math.max((int)param, (int)secondParam);
        case LONG:    return Math.max((long)param, (long)secondParam);
        default:
          throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }
    }
  };
  @ElFunction(
    prefix = "math",
    name = "max",
    description = "Returns the greater of two numbers."
  )
  public static Object max(@ElParam("Number 1") Object number, @ElParam("Number 2") Object number2) {
      return MAX.process(number, number2);
  }

  /*
   * Math:min
   */
  private static Operation MIN = new Operation("min") {
    @Override
    protected Object calculate(GivenType type, Object param, Object secondParam) {
      switch (type) {
        case DOUBLE:  return Math.min((double)param, (double)secondParam);
        case FLOAT:   return Math.min((float)param, (double)secondParam);
        case INT:     return Math.min((int)param, (int)secondParam);
        case LONG:    return Math.min((long)param, (long)secondParam);
        default:
          throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }
    }
  };
  @ElFunction(
    prefix = "math",
    name = "min",
    description = "Return minimal value of given two numbers."
  )
  public static Object min(@ElParam("Number 1") Object number, @ElParam("Number 2") Object number2) {
      return MIN.process(number, number2);
  }

  /*
   * Math:round
   */
  private static Operation ROUND = new Operation("round") {
    @Override
    protected Object calculate(GivenType type, Object param, Object secondParam) {
      switch (type) {
        case DOUBLE:  return Math.round((double)param);
        case FLOAT:   return Math.round((float)param);
        default:
          throw new IllegalArgumentException(Utils.format("Unsupported data type {} for operation {} and value '{}'", param.getClass(), operationName, param));
      }
    }
  };
  @ElFunction(
    prefix = "math",
    name = "round",
    description = "Returns the closest int or long to the argument, with ties rounding up."
  )
  public static Object round(@ElParam("number") Object number) {
      return ROUND.process(number);
  }

  private MathEL() {
  }

}
