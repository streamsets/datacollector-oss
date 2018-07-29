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
package com.streamsets.pipeline.lib.jdbc.typesupport;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;

import java.util.HashMap;
import java.util.Map;

public final class DecimalJdbcTypeSupport extends PrimitiveJdbcTypeSupport {
  public static final String SCALE = "scale";
  public static final String PRECISION = "precision";
  private static final int MAX_SCALE_PRECISION = 38;

  @Override
  protected Field generateExtraInfoFieldForMetadataRecord(JdbcTypeInfo JdbcTypeInfo) {
    Map<String, Field> extraInfo = new HashMap<>();
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)JdbcTypeInfo;
    extraInfo.put(SCALE, Field.create(decimalTypeInfo.getScale()));
    extraInfo.put(PRECISION, Field.create(decimalTypeInfo.getPrecision()));
    return Field.create(extraInfo);
  }

  @Override
  protected DecimalTypeInfo generateJdbcTypeInfoFromMetadataField(JdbcType type, Field JdbcTypeField, JdbcSchemaWriter schemaWriter)
      throws JdbcStageCheckedException {
    Map<String, Field> extraInfo = JdbcTypeField.getValueAsMap();
    if(!extraInfo.containsKey(SCALE)) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_301, Utils.format("Missing {} in field {}", SCALE, JdbcTypeField));
    }
    if(!extraInfo.containsKey(PRECISION)) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_301, Utils.format("Missing {} in field {}", PRECISION, JdbcTypeField));
    }
    int scale = extraInfo.get(SCALE).getValueAsInteger();
    int precision = extraInfo.get(PRECISION).getValueAsInteger();
    return new DecimalTypeInfo(schemaWriter, precision, scale);
  }

  @Override
  public DecimalTypeInfo generateJdbcTypeInfoFromResultSet(String JdbcTypeString, JdbcSchemaWriter schemaWriter)
      throws JdbcStageCheckedException {
    JdbcType type = JdbcType.prefixMatch(JdbcTypeString);
    if (type != JdbcType.DECIMAL) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_301, "Invalid Column Type Definition: " + JdbcTypeString);
    }
    String scalePrecision  = JdbcTypeString.substring(type.name().length() + 1, JdbcTypeString.length() - 1);
    String split[]  = scalePrecision.split(COMMA);
    if (split.length != 2) {
      throw new JdbcStageCheckedException(JdbcErrors.JDBC_301, "Invalid Column Type Definition: " + JdbcTypeString);
    }
    int precision = Integer.parseInt(split[0]);
    int scale = Integer.parseInt(split[1]);
    return new DecimalTypeInfo(schemaWriter, precision, scale);
  }

  private static int getScale(Object... auxillaryArgs) {
    if (auxillaryArgs != null) {
      return (int)auxillaryArgs[1];
    }
    return MAX_SCALE_PRECISION;
  }

  private static int getPrecision(Object... auxillaryArgs) {
    if (auxillaryArgs != null) {
      return (int)auxillaryArgs[0];
    }
    return MAX_SCALE_PRECISION;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DecimalTypeInfo generateJdbcTypeInfoFromRecordField(Field field, JdbcSchemaWriter schemaWriter, Object... auxillaryArgs)
      throws JdbcStageCheckedException {
    int precision = getPrecision(auxillaryArgs);
    int scale = getScale(auxillaryArgs);
    return new DecimalTypeInfo(schemaWriter, precision, scale);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DecimalTypeInfo createTypeInfo(JdbcType jdbcType, JdbcSchemaWriter schemaWriter, Object... auxillaryArgs) {
    return new DecimalTypeInfo(schemaWriter, getPrecision(auxillaryArgs), getScale(auxillaryArgs));
  }

  public static class DecimalTypeInfo extends PrimitiveJdbcTypeInfo {
    private int precision;
    private int scale;

    public DecimalTypeInfo(JdbcSchemaWriter schemaWriter, int precision, int scale) {
      super(JdbcType.DECIMAL, schemaWriter);
      this.precision = precision;
      this.scale = scale;
    }

    public int getScale() {
      return scale;
    }

    public int getPrecision() {
      return precision;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      DecimalTypeInfo that = (DecimalTypeInfo) o;

      if (scale != that.scale) return false;
      return precision == that.precision;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + scale;
      result = 31 * result + precision;
      return result;
    }

    @Override
    public String toString() {
      return super.toString() + OPEN_BRACKET +
          precision + COMMA + scale + CLOSE_BRACKET;
    }
  }
}
