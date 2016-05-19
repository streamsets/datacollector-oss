/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.lib.hive;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.destination.hive.Errors;

import java.util.LinkedHashMap;

public class AvroSchemaInfoCacheSupport implements HMSCacheSupport<AvroSchemaInfoCacheSupport.AvroSchemaInfo,
    AvroSchemaInfoCacheSupport.AvroSchemaInfoCacheLoader> {

  private static LinkedHashMap<String, AvroSchemaInfo> avroSchemaRepo = new LinkedHashMap<>();

  @Override
  public AvroSchemaInfoCacheLoader newHMSCacheLoader(
      String jdbcUrl,
      String qualifiedTableName,
      Object... auxiliaryInfo)
  {
    return new AvroSchemaInfoCacheLoader(jdbcUrl, qualifiedTableName);
  }

  @Override
  public Cache<String, Optional<AvroSchemaInfo>> createCache(int maxCacheSize) {
    return CacheBuilder.<String, Optional<AvroSchemaInfo>>newBuilder().maximumSize(maxCacheSize).build();
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
  }

  public class AvroSchemaInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<AvroSchemaInfo> {

    protected AvroSchemaInfoCacheLoader(String jdbcUrl, String qualifiedTableName) {
      super(jdbcUrl, qualifiedTableName);
    }
    @Override
    protected AvroSchemaInfo loadHMSCacheInfo() throws StageException {
      // here I will put Infer Avro Schema mechanism and return AvroSchemaInfo
      return null;
    }
  }

}
