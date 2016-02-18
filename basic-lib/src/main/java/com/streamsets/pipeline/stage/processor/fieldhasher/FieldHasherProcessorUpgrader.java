/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Upgrader for FieldHasherProcessor.
 */
public class FieldHasherProcessorUpgrader implements StageUpgrader{
  private static final Joiner JOINER = Joiner.on(".");
  //v1 to v2 constants
  //old fields
  private static final String FIELD_HASHER_CONFIGS = "fieldHasherConfigs";
  private static final String FIELDS_TO_HASH = "fieldsToHash";
  //New fields
  private static final String HASHER_CONFIG = "hasherConfig";
  private static final String RECORD_HASHER_CONFIG = "recordHasherConfig";
  private static final String TARGET_FIELD_HASHER_CONFIGS = "targetFieldHasherConfigs";
  private static final String INPLACE_FIELD_HASHER_CONFIGS = "inPlaceFieldHasherConfigs";
  private static final String SOURCE_FIELDS_TO_HASH = "sourceFieldsToHash";
  private static final String HASH_ENTIRE_RECORD = "hashEntireRecord";
  private static final String INCLUDE_RECORD_HEADER = "includeRecordHeaderForHashing";
  private static final String HASH_TYPE = "hashType";
  private static final String TARGET_FIELD = "targetField";
  private static final String HEADER_ATTRIBUTE = "headerAttribute";


  @Override
  public List<Config> upgrade (
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<Config>();
    List<Config> configsToAdd = new ArrayList<Config>();
    for (Config config : configs) {
      switch (config.getName()) {
        case FIELD_HASHER_CONFIGS:
          configsToRemove.add(config);

          //Handle InPlace Field Hasher Config
          //Move fieldHasherConfigs to the new config hasherConfig.inPlaceFieldHasherConfigs
          List<LinkedHashMap<String, Object>> fieldHasherConfigs =
              (List<LinkedHashMap<String, Object>>) config.getValue();
          for (LinkedHashMap<String, Object> fieldHasherConfig : fieldHasherConfigs) {
            List<String> fieldsToHash = (List<String>) fieldHasherConfig.get(FIELDS_TO_HASH);
            fieldHasherConfig.remove(FIELDS_TO_HASH);
            fieldHasherConfig.put(SOURCE_FIELDS_TO_HASH, fieldsToHash);
          }
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, INPLACE_FIELD_HASHER_CONFIGS),
                  fieldHasherConfigs
              ));

          //Add new Record Hasher Config
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HASH_ENTIRE_RECORD),
                  false
              )
          );
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, INCLUDE_RECORD_HEADER),
                  false
              )
          );
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HASH_TYPE),
                  HashType.MD5
              )
          );
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, TARGET_FIELD),
                  ""
              )
          );
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HEADER_ATTRIBUTE),
                  ""
              )
          );

          //Add new Target Field Hasher Config
          configsToAdd.add(
              new Config(
                  JOINER.join(HASHER_CONFIG, TARGET_FIELD_HASHER_CONFIGS),
                  new ArrayList<LinkedHashMap<String, Object>>()
              )
          );
          break;
        default: //NO OP for others
      }
    }
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
