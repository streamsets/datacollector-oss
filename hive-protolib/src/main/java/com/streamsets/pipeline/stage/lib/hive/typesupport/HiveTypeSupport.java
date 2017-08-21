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
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;

import java.util.HashMap;
import java.util.Map;

/**
 * Interface for supporting HiveType.
 */
public abstract class HiveTypeSupport {

  /**
   * Generate Hive Type Info Representation inside the Metadata Record.
   * @param hiveTypeInfo {@link HiveTypeInfo}
   * @return {@link Field} which is the field in the metadata record which represents {@link HiveTypeInfo}
   */
  public Field generateHiveTypeInfoFieldForMetadataRecord(HiveTypeInfo hiveTypeInfo) {
    Map<String, Field> fields = new HashMap<>();
    fields.put(HiveMetastoreUtil.TYPE, Field.create(hiveTypeInfo.getHiveType().name()));
    fields.put(HiveMetastoreUtil.EXTRA_INFO, generateExtraInfoFieldForMetadataRecord(hiveTypeInfo));
    fields.put(HiveMetastoreUtil.COMMENT, Field.create(Field.Type.STRING, hiveTypeInfo.getComment()));
    return Field.create(fields);
  }

  /**
   * Generate {@link HiveTypeInfo} from the Metadata Record <br>.
   * (Reverse of {@link #generateHiveTypeInfoFieldForMetadataRecord(HiveTypeInfo)})
   *
   * @throws StageException if the metadata field is not valid.
   * @param hiveTypeInfoField hiveTypeInfoField
   * @return {@link HiveTypeInfo}
   * @throws StageException If the record has invalid
   */
  @SuppressWarnings("unchecked")
  public HiveTypeInfo generateHiveTypeInfoFromMetadataField(Field hiveTypeInfoField) throws StageException {
    if (hiveTypeInfoField.getType() == Field.Type.MAP) {
      Map<String, Field> fields = (Map<String, Field>) hiveTypeInfoField.getValue();
      if (!fields.containsKey(HiveMetastoreUtil.TYPE)
          || !fields.containsKey(HiveMetastoreUtil.EXTRA_INFO)) {
        throw new StageException(Errors.HIVE_17, HiveMetastoreUtil.TYPE_INFO);
      }
      HiveType hiveType = HiveType.getHiveTypeFromString(fields.get(HiveMetastoreUtil.TYPE).getValueAsString());
      String comment = "";
      if(fields.containsKey(HiveMetastoreUtil.COMMENT)) {
        comment = fields.get(HiveMetastoreUtil.COMMENT).getValueAsString();
      }
      return generateHiveTypeInfoFromMetadataField(hiveType, comment, fields.get(HiveMetastoreUtil.EXTRA_INFO));
    } else {
      throw new StageException(Errors.HIVE_17, HiveMetastoreUtil.TYPE_INFO);
    }
  }

  /**
   * Generate Column Definition for create table / add columns
   * @param hiveTypeInfo {@link HiveTypeInfo}
   * @param columnName Column Name
   * @return Column definition
   */
  public String generateColumnTypeDefinition(HiveTypeInfo hiveTypeInfo, String columnName) {
    return String.format(
      HiveMetastoreUtil.COLUMN_TYPE,
      columnName,
      hiveTypeInfo.toString(),
      hiveTypeInfo.getComment()
    );
  }

  /**
   * Generate {@link HiveTypeInfo} from Metadata record field.
   * @param type {@link HiveType}
   * @param hiveTypeField hive type field
   * @param <T> {@link HiveTypeInfo}
   * @return {@link HiveTypeInfo}
   */
  protected abstract <T extends HiveTypeInfo> T generateHiveTypeInfoFromMetadataField(
      HiveType type,
      String comment,
      Field hiveTypeField
  ) throws StageException;

  /**
   * Generate Metdata Record Extra Info Field from {@link HiveTypeInfo}.
   * @param hiveTypeInfo {@link HiveTypeInfo}
   * @param <T> {@link HiveTypeInfo}
   * @return {@link Field} of Extra Info for the hive type inside the metadata record
   */
  protected abstract <T extends HiveTypeInfo> Field generateExtraInfoFieldForMetadataRecord(T hiveTypeInfo);

  /**
   * Returns the {@link HiveTypeInfo} from the information in the result set of desc query.
   * @param hiveTypeString Column definition from the SQL Result Set.
   * @param <T> {@link HiveTypeInfo}
   * @return {@link HiveTypeInfo}
   * @throws StageException
   */
  public abstract <T extends HiveTypeInfo> T generateHiveTypeInfoFromResultSet(String hiveTypeString)
      throws HiveStageCheckedException;

  /**
   * Get the {@link HiveTypeInfo} from the record field
   * @param field Field inside a record for which HiveTypeInfo need to be extracted.
   * @param <T> {@link HiveTypeInfo}
   * @return {@link HiveTypeInfo}
   * @throws StageException
   */
  public abstract <T extends HiveTypeInfo> T generateHiveTypeInfoFromRecordField(Field field, String comment, Object... auxillaryArgs)
      throws HiveStageCheckedException;

  /**
   * Create a new {@link HiveTypeInfo}
   * @param <T> {@link HiveTypeInfo}
   * @return {@link HiveTypeInfo}
   */
  public abstract <T extends HiveTypeInfo> T createTypeInfo(HiveType hiveType, String comment, Object... auxillaryArgs);
}
