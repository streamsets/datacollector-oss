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
package com.streamsets.pipeline.stage.lib.hive.cache;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;


public class AvroSchemaInfoCacheSupport implements HMSCacheSupport<AvroSchemaInfoCacheSupport.AvroSchemaInfo,
    AvroSchemaInfoCacheSupport.AvroSchemaInfoCacheLoader> {

  @Override
  public AvroSchemaInfoCacheLoader newHMSCacheLoader(HiveQueryExecutor executor) {
    return new AvroSchemaInfoCacheLoader(executor);
  }

  public static class AvroSchemaInfo extends HMSCacheSupport.HMSCacheInfo<String> {

    public AvroSchemaInfo(String schema) {
      super(schema);
    }

    @Override
    public String getDiff(String newState) throws StageException{
      // we don't perform diff on schema. This function should not be called
      throw new StageException(Errors.HIVE_01, newState, "Invalid operation");
    }

    @Override
    public void updateState(String newSchema) {
      state = newSchema;
    }

    public final String getSchema() { return state; }
  }

  public class AvroSchemaInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<AvroSchemaInfo> {

    protected AvroSchemaInfoCacheLoader(HiveQueryExecutor executor) {
      super(executor);
    }

    @Override
    protected AvroSchemaInfo loadHMSCacheInfo(String qualifiedTableName) throws StageException {
      // we don't perform load on avroSchema. This function should not be called
      throw new StageException(Errors.HIVE_01,  "Invalid operation");
    }
  }

}
