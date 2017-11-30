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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;

import java.util.LinkedHashMap;

public class PrimitiveJdbcTypeSupport extends JdbcTypeSupport {
  @Override
  protected Field generateExtraInfoFieldForMetadataRecord(JdbcTypeInfo jdbcTypeInfo) {
    return Field.create(new LinkedHashMap<>());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected PrimitiveJdbcTypeInfo generateJdbcTypeInfoFromMetadataField(JdbcType type, Field hiveTypeField, JdbcSchemaWriter schemaWriter) throws StageException {
    return new PrimitiveJdbcTypeInfo(type, schemaWriter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveJdbcTypeInfo generateJdbcTypeInfoFromResultSet(String jdbcTypeString, JdbcSchemaWriter schemaWriter)
      throws JdbcStageCheckedException {
    JdbcType type = JdbcType.prefixMatch(jdbcTypeString);
    return new PrimitiveJdbcTypeInfo(type, schemaWriter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveJdbcTypeInfo generateJdbcTypeInfoFromRecordField(Field field, JdbcSchemaWriter schemaWriter, Object... auxillaryArgs)
      throws JdbcStageCheckedException{
    return new PrimitiveJdbcTypeInfo(JdbcType.getJdbcTypeforFieldType(field.getType()),schemaWriter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveJdbcTypeInfo createTypeInfo(JdbcType jdbcType, JdbcSchemaWriter schemaWriter, Object... auxillaryArgs){
    return new PrimitiveJdbcTypeInfo(jdbcType, schemaWriter);
  }

  public static class PrimitiveJdbcTypeInfo extends JdbcTypeInfo {
    public PrimitiveJdbcTypeInfo(JdbcType jdbcType, JdbcSchemaWriter schemaWriter) {
      super(jdbcType, schemaWriter);
    }
  }
}
