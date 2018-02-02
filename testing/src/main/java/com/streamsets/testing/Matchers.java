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
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
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
          return value.equals(getValueFromField(field));
        }
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Field of type ").appendText(type.name()).appendText(" with value ").appendValue(value);
    }

    protected abstract Object getValueFromField(Field field);
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
}
