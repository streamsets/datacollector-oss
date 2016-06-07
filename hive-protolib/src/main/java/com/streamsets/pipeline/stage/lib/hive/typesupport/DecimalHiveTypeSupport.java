/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.lib.hive.typesupport;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public final class DecimalHiveTypeSupport extends PrimitiveHiveTypeSupport {
  private static final String SCALE = "scale";
  private static final String PRECISION = "precision";

  @Override
  protected Field generateExtraInfoFieldForMetadataRecord(HiveTypeInfo hiveTypeInfo) {
    Map<String, Field> extraInfo = new HashMap<>();
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)hiveTypeInfo;
    extraInfo.put(SCALE, Field.create(decimalTypeInfo.getScale()));
    extraInfo.put(PRECISION, Field.create(decimalTypeInfo.getPrecision()));
    return Field.create(extraInfo);
  }

  @Override
  protected DecimalTypeInfo generateHiveTypeInfoFromMetadataField(HiveType type, Field hiveTypeField) throws StageException {
    Map<String, Field> extraInfo = hiveTypeField.getValueAsMap();
    if(!extraInfo.containsKey(SCALE)) {
      throw new StageException(Errors.HIVE_01, Utils.format("Missing {} in field {}", SCALE, hiveTypeField));
    }
    if(!extraInfo.containsKey(PRECISION)) {
      throw new StageException(Errors.HIVE_01, Utils.format("Missing {} in field {}", PRECISION, hiveTypeField));
    }
    int scale = extraInfo.get(SCALE).getValueAsInteger();
    int precision = extraInfo.get(PRECISION).getValueAsInteger();
    return new DecimalTypeInfo(scale, precision);
  }

  @Override
  public DecimalTypeInfo generateHiveTypeInfoFromResultSet(String hiveTypeString) throws StageException {
    HiveType type = HiveType.prefixMatch(hiveTypeString);
    if (type != HiveType.DECIMAL) {
      throw new StageException(Errors.HIVE_01, "Invalid Column Type Definition: " + hiveTypeString);
    }
    String scalePrecision  = hiveTypeString.substring(type.name().length() + 1, hiveTypeString.length() - 1);
    String split[]  = scalePrecision.split(HiveMetastoreUtil.COMMA);
    if (split.length != 2) {
      throw new StageException(Errors.HIVE_01, "Invalid Column Type Definition: " + hiveTypeString);
    }
    int scale = Integer.parseInt(split[0]);
    int precision = Integer.parseInt(split[1]);
    return new DecimalTypeInfo(scale, precision);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DecimalTypeInfo generateHiveTypeInfoFromRecordField(Field field) throws StageException{
    BigDecimal bigDecimal = field.getValueAsDecimal();
    return new DecimalTypeInfo(bigDecimal.scale(), bigDecimal.precision());
  }

  @Override
  @SuppressWarnings("unchecked")
  public DecimalTypeInfo createTypeInfo(HiveTypeConfig typeConfig){
    return new DecimalTypeInfo(typeConfig.scale, typeConfig.precision);
  }

  public static class DecimalTypeInfo extends PrimitiveHiveTypeInfo {
    private int scale;
    private int precision;

    public DecimalTypeInfo(int scale, int precision) {
      super(HiveType.DECIMAL);
      this.scale = scale;
      this.precision = precision;
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
