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
package com.streamsets.pipeline.stage.lib.hive.typesupport;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;

import java.util.HashMap;
import java.util.Map;

public final class DecimalHiveTypeSupport extends PrimitiveHiveTypeSupport {
  public static final String SCALE = "scale";
  public static final String PRECISION = "precision";
  private static final int MAX_SCALE_PRECISION = 38;

  @Override
  protected Field generateExtraInfoFieldForMetadataRecord(HiveTypeInfo hiveTypeInfo) {
    Map<String, Field> extraInfo = new HashMap<>();
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)hiveTypeInfo;
    extraInfo.put(SCALE, Field.create(decimalTypeInfo.getScale()));
    extraInfo.put(PRECISION, Field.create(decimalTypeInfo.getPrecision()));
    return Field.create(extraInfo);
  }

  @Override
  protected DecimalTypeInfo generateHiveTypeInfoFromMetadataField(HiveType type, String comment, Field hiveTypeField)
      throws HiveStageCheckedException {
    Map<String, Field> extraInfo = hiveTypeField.getValueAsMap();
    if(!extraInfo.containsKey(SCALE)) {
      throw new HiveStageCheckedException(Errors.HIVE_01, Utils.format("Missing {} in field {}", SCALE, hiveTypeField));
    }
    if(!extraInfo.containsKey(PRECISION)) {
      throw new HiveStageCheckedException(Errors.HIVE_01, Utils.format("Missing {} in field {}", PRECISION, hiveTypeField));
    }
    int scale = extraInfo.get(SCALE).getValueAsInteger();
    int precision = extraInfo.get(PRECISION).getValueAsInteger();
    return new DecimalTypeInfo(comment, precision, scale);
  }

  @Override
  public DecimalTypeInfo generateHiveTypeInfoFromResultSet(String hiveTypeString)
      throws HiveStageCheckedException {
    HiveType type = HiveType.prefixMatch(hiveTypeString);
    if (type != HiveType.DECIMAL) {
      throw new HiveStageCheckedException(Errors.HIVE_01, "Invalid Column Type Definition: " + hiveTypeString);
    }
    String scalePrecision  = hiveTypeString.substring(type.name().length() + 1, hiveTypeString.length() - 1);
    String split[]  = scalePrecision.split(HiveMetastoreUtil.COMMA);
    if (split.length != 2) {
      throw new HiveStageCheckedException(Errors.HIVE_01, "Invalid Column Type Definition: " + hiveTypeString);
    }
    int precision = Integer.parseInt(split[0]);
    int scale = Integer.parseInt(split[1]);
    return new DecimalTypeInfo("", precision, scale);
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
  public DecimalTypeInfo generateHiveTypeInfoFromRecordField(Field field, String comment, Object... auxillaryArgs)
      throws HiveStageCheckedException {
    int precision = getPrecision(auxillaryArgs);
    int scale = getScale(auxillaryArgs);
    return new DecimalTypeInfo(comment, precision, scale);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DecimalTypeInfo createTypeInfo(HiveType hiveType, String comment, Object... auxillaryArgs) {
    return new DecimalTypeInfo(comment, getPrecision(auxillaryArgs), getScale(auxillaryArgs));
  }

  public static class DecimalTypeInfo extends PrimitiveHiveTypeInfo {
    private int precision;
    private int scale;

    public DecimalTypeInfo(String comment, int precision, int scale) {
      super(HiveType.DECIMAL, comment);
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
      return hiveType.name() + HiveMetastoreUtil.OPEN_BRACKET +
          precision + HiveMetastoreUtil.COMMA + scale + HiveMetastoreUtil.CLOSE_BRACKET;
    }
  }
}
