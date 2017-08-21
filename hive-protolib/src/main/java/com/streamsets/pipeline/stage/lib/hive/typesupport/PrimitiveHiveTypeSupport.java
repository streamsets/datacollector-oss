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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;

import java.util.LinkedHashMap;

public class PrimitiveHiveTypeSupport extends HiveTypeSupport{
  @Override
  protected Field generateExtraInfoFieldForMetadataRecord(HiveTypeInfo hiveTypeInfo) {
    return Field.create(new LinkedHashMap<>());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected PrimitiveHiveTypeInfo generateHiveTypeInfoFromMetadataField(HiveType type, String comment, Field hiveTypeField) throws StageException {
    return new PrimitiveHiveTypeInfo(type, comment);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveHiveTypeInfo generateHiveTypeInfoFromResultSet(String hiveTypeString)
      throws HiveStageCheckedException {
    HiveType type = HiveType.prefixMatch(hiveTypeString);
    return new PrimitiveHiveTypeInfo(type, "");
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveHiveTypeInfo generateHiveTypeInfoFromRecordField(Field field, String comment, Object... auxillaryArgs)
      throws HiveStageCheckedException{
    return new PrimitiveHiveTypeInfo(HiveType.getHiveTypeforFieldType(field.getType()), comment);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveHiveTypeInfo createTypeInfo(HiveType hiveType, String comment, Object... auxillaryArgs){
    return new PrimitiveHiveTypeInfo(hiveType, comment);
  }

  public static class PrimitiveHiveTypeInfo extends HiveTypeInfo{
    public PrimitiveHiveTypeInfo(HiveType hiveType, String comment) {
      super(hiveType, comment);
    }
  }
}
