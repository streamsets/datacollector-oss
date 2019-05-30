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

package com.streamsets.testing;

import com.streamsets.pipeline.api.Field;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Matchers {

  public static Matcher<Field> listFieldWithValues(Object... values) {
    return listFieldWithValues(Arrays.asList(values));
  }

  public static Matcher<Field> listFieldWithValues(List<?> values) {
    return new FieldMatcher(Field.Type.LIST, values) {
      @Override
      protected Object getValueFromField(Field field) {
        return LIST_ITEMS_ACCESSOR.getValue(field);
      }
    };
  }

  public static Matcher<Field> listFieldWithSize(final int size) {
    return new BaseMatcher<Field>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("List field with size ").appendValue(size);
      }

      @Override
      public boolean matches(Object item) {
        if (item != null && item instanceof Field) {
          final List<Field> valueAsList = ((Field) item).getValueAsList();
          return valueAsList != null && valueAsList.size() == size;
        }
        return false;
      }
    };
  }

  private static ValueAccessor<Object> LIST_ITEMS_ACCESSOR = field -> {
    final List<Object> fieldValues = new LinkedList<>();
    for (Field childField : field.getValueAsList()) {
      fieldValues.add(childField.getValue());
    }
    return fieldValues;
  };

  public static Matcher<Field> fieldWithValue(final String value) {
    return new FieldMatcher(Field.Type.STRING, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsString();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final int value) {
    return new FieldMatcher(Field.Type.INTEGER, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsInteger();
      }
    };
  }

  public static Matcher<Field> dateFieldWithValue(final Date date) {
    return new FieldMatcher(Field.Type.DATE, date) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsDate();
      }
    };
  }

  public static Matcher<Field> dateTimeFieldWithValue(final Date dateTime) {
    return new FieldMatcher(Field.Type.DATETIME, dateTime) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsDatetime();
      }
    };
  }

  public static Matcher<Field> zonedDateTimeUTCFieldWithValue(final long epochSeconds, final long micros) {
    return new FieldMatcher(Field.Type.ZONED_DATETIME, ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(epochSeconds, micros),
        ZoneId.of("Z")
    )) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsZonedDateTime();
      }
    };
  }

  public static Matcher<Field> intFieldWithNullValue() {
    return new FieldMatcher(Field.Type.INTEGER, null) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValue();
      }
    };
  }

  public static Matcher<Field> stringFieldWithNullValue() {
    return new FieldMatcher(Field.Type.STRING, null) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValue();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final float value) {
    return new FieldMatcher(Field.Type.FLOAT, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsFloat();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final double value) {
    return new FieldMatcher(Field.Type.DOUBLE, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsDouble();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final long value) {
    return new FieldMatcher(Field.Type.LONG, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsLong();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final Date value) {
    return new FieldMatcher(Field.Type.DATE, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsDate();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final ZonedDateTime value) {
    return new FieldMatcher(Field.Type.ZONED_DATETIME, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsZonedDateTime();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final BigDecimal value) {
    return new FieldMatcher(Field.Type.DECIMAL, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsDecimal();
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final byte[] value) {
    return new FieldMatcher(Field.Type.BYTE_ARRAY, value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValueAsByteArray();
      }

      @Override
      protected int getValueArrayLengthFromField(Field field) {
        return field.getValueAsByteArray().length;
      }
    };
  }

  public static Matcher<Field> fieldWithValue(final Object value) {
    return new FieldMatcher(getTypeFromObject(value), value) {
      @Override
      protected Object getValueFromField(Field field) {
        return field.getValue();
      }
    };
  }

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, final int value) {
    return new MapFieldWithEntryMatcher(nestedFieldName, value, Field::getValueAsInteger);
  }

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, final long value) {
    return new MapFieldWithEntryMatcher(nestedFieldName, value, Field::getValueAsLong);
  }

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, final String value) {
    return new MapFieldWithEntryMatcher(nestedFieldName, value, Field::getValueAsString);
  }

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, final double value) {
    return new MapFieldWithEntryMatcher(nestedFieldName, value, Field::getValueAsDouble);
  }

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, final BigDecimal value) {
    return new MapFieldWithEntryMatcher(nestedFieldName, value, Field::getValueAsDecimal);
  }

  // TODO: make this work properly (ex: fix issue below)
  /*
  java.lang.AssertionError:
Expected: Field of type MAP or LIST_MAP with field entry named values having value <[1, 2, 3]>
     but: was <Field[MAP:{values=Field[LIST:[Field[INTEGER:1], Field[INTEGER:2], Field[INTEGER:3]]]}]>
Expected :Field of type MAP or LIST_MAP with field entry named values having value <[1, 2, 3]>
   */

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, Object... values) {
    return new MapFieldWithEntryMatcher(nestedFieldName, Arrays.asList(values), LIST_ITEMS_ACCESSOR);
  }

  public static Matcher<Field> mapFieldWithEntry(final String nestedFieldName, final List<?> value) {
    return new MapFieldWithEntryMatcher(nestedFieldName, value, LIST_ITEMS_ACCESSOR);
  }

  private abstract static class FieldMatcher extends BaseMatcher<Field> {
    private final Field.Type type;
    private final Object value;

    FieldMatcher(Field.Type type, Object value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public boolean matches(Object item) {
      if (item instanceof Field) {
        Field field = (Field) item;
        if (field.getType() == type) {
          final Object valueFromField = getValueFromField(field);
          if (value == null) {
            return valueFromField == null;
          } else if (value.getClass().isArray()) {
            // arrays are a pain...
            if (byte.class.equals(value.getClass().getComponentType())) {
              return Arrays.equals((byte[]) value, (byte[]) valueFromField);
            } else {
              throw new IllegalStateException(
                  "Incomparable array type: " + value.getClass().getComponentType().toString()
              );
            }
          } else {
            return value.equals(valueFromField);
          }
        }
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Field of type ").appendText(type.name()).appendText(" with value ").appendValue(value);
    }

    protected abstract Object getValueFromField(Field field);
    protected int getValueArrayLengthFromField(Field field) {
      return -1;
    }
  }

  private static class MapFieldWithEntryMatcher<VT> extends BaseMatcher<Field> {
    private final String nestedFieldName;
    private final VT expectedValue;
    private final ValueAccessor<VT> valueAccessor;

    MapFieldWithEntryMatcher(String nestedFieldName, VT expectedValue, ValueAccessor<VT> valueAccessor) {
      this.nestedFieldName = nestedFieldName;
      this.expectedValue = expectedValue;
      this.valueAccessor = valueAccessor;
    }

    @Override
    public boolean matches(Object item) {
      if (item instanceof Field) {
        Field field = (Field) item;
        if (field.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
          final Map<String, Field> childFields = field.getValueAsMap();
          if (!childFields.containsKey(nestedFieldName)) {
            return false;
          }
          final VT value = valueAccessor.getValue(childFields.get(nestedFieldName));
          if (expectedValue == null) {
            return value == null;
          } else {
            return expectedValue.equals(value);
          }
        }
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(String.format(
          "Field of type MAP or LIST_MAP with field entry named %s having value ",
          nestedFieldName
      )).appendValue(expectedValue);
    }
  }

  /**
   * Similar to {@link org.hamcrest.core.StringContains#containsString(String)} but case-insensitive
   *
   * Adapted from
   * <a href="https://gist.github.com/spuklo/660c4504d088a1d7f38f#file-caseinsensitivesubstringmatcher-java">
   *   https://gist.github.com/spuklo/660c4504d088a1d7f38f#file-caseinsensitivesubstringmatcher-java
   *   </a>
   */
  private static class CaseInsensitiveSubstringMatcher extends TypeSafeMatcher<String> {
    private final String subString;

    private CaseInsensitiveSubstringMatcher(final String subString) {
      this.subString = subString;
    }

    @Override
    protected boolean matchesSafely(final String actualString) {
      return actualString.toLowerCase().contains(this.subString.toLowerCase());
    }

    @Override
    public void describeTo(final Description description) {
      description.appendText("containing substring \"" + this.subString + "\" (case-insensitive)");
    }
  }

  @Factory
  public static Matcher<String> containsIgnoringCase(final String subString) {
    return new CaseInsensitiveSubstringMatcher(subString);
  }

  public static Matcher<Integer> hasSameSignAs(final int value) {
    return new TypeSafeMatcher<Integer>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("having the same sign as ");
        description.appendValue(value);
        description.appendText(", which is ");
        description.appendValue(Integer.signum(value));
      }

      @Override
      protected boolean matchesSafely(Integer item) {
        return Integer.signum(value) == Integer.signum(item);
      }
    };
  }

  /**
   * Returns the {@link Field.Type} corresponding to the given value's class
   *
   * This is copy-pasted from FieldUtils class, which lives in the stagesupport module
   *
   * Due to module circular dependencies, we cannot depend on it from here easily
   *
   * TODO: remove this once SDC-11502 is done
   *
   * @param value the value
   * @return the {@link Field.Type} for the given value param
   */
  public static Field.Type getTypeFromObject(Object value) {
    if(value instanceof Double) {
      return Field.Type.DOUBLE;
    } else if(value instanceof Long) {
      return Field.Type.LONG;
    } else if(value instanceof BigDecimal) {
      return Field.Type.DECIMAL;
    } else if(value instanceof Date) {
      //This can only happen in ${time:now()}
      return Field.Type.DATETIME;
      //For all the timeEL, we currently return String so we are safe.
    } else if(value instanceof Short) {
      return Field.Type.SHORT;
    } else if(value instanceof Boolean) {
      return Field.Type.BOOLEAN;
    } else if(value instanceof Byte) {
      return Field.Type.BYTE;
    } else if(value instanceof byte[]) {
      return Field.Type.BYTE_ARRAY;
    } else if(value instanceof Character) {
      return Field.Type.CHAR;
    } else if(value instanceof Float) {
      return Field.Type.FLOAT;
    } else if(value instanceof Integer) {
      return Field.Type.INTEGER;
    } else if(value instanceof String) {
      return Field.Type.STRING;
    } else if(value instanceof LinkedHashMap) {
      return Field.Type.LIST_MAP;
    } else if(value instanceof Map) {
      return Field.Type.MAP;
    } else if(value instanceof List) {
      return Field.Type.LIST;
    }
    return Field.Type.STRING;
  }
}
