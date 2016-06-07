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

import java.util.LinkedHashMap;

public class PrimitiveHiveTypeSupport extends HiveTypeSupport{
  @Override
  protected Field generateExtraInfoFieldForMetadataRecord(HiveTypeInfo hiveTypeInfo) {
    return Field.create(new LinkedHashMap<String, Field>());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected PrimitiveHiveTypeInfo generateHiveTypeInfoFromMetadataField(HiveType type, Field hiveTypeField) throws StageException {
    return new PrimitiveHiveTypeInfo(type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveHiveTypeInfo generateHiveTypeInfoFromResultSet(String hiveTypeString) throws StageException {
    HiveType type = HiveType.prefixMatch(hiveTypeString);
    return new PrimitiveHiveTypeInfo(type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveHiveTypeInfo generateHiveTypeInfoFromRecordField(Field field) throws StageException{
    return new PrimitiveHiveTypeInfo(HiveType.getHiveTypeforFieldType(field.getType()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveHiveTypeInfo createTypeInfo(HiveTypeConfig hiveTypeConfig){
    return new PrimitiveHiveTypeInfo(hiveTypeConfig.valueType);
  }

  public static class PrimitiveHiveTypeInfo extends HiveTypeInfo{
    public PrimitiveHiveTypeInfo(HiveType hiveType) {
      super(hiveType);
    }
  }
}
