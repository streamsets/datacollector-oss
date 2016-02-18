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
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldHasherProcessor extends SingleLaneRecordProcessor {
  private final HasherConfig hasherConfig;
  private final OnStagePreConditionFailure onStagePreConditionFailure;
  private static final Joiner JOINER = Joiner.on(".");

  public FieldHasherProcessor(
      HasherConfig hasherConfig,
      OnStagePreConditionFailure onStagePreConditionFailure) {
    this.hasherConfig = hasherConfig;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
  }

  private void validateTargetFieldHeaderAttribute(
      String targetFieldOrHeaderAttr,
      String group,
      String configPrefix,
      String configName,
      List<ConfigIssue> configIssues) {
    if (!targetFieldOrHeaderAttr.isEmpty() && FieldRegexUtil.hasWildCards(targetFieldOrHeaderAttr)) {
      configIssues.add(
          getContext().createConfigIssue(
              group,
              JOINER.join(configPrefix, configName),
              Errors.HASH_02,
              configName
          )
      );
    }
  }

  private void validateTarget(
      String targetField,
      String headerAttribute,
      String group,
      String configPrefix,
      List<ConfigIssue> configIssues
  ) {
    if (targetField.isEmpty() && headerAttribute.isEmpty()) {
      configIssues.add(
          getContext().createConfigIssue(
              group,
              configPrefix,
              Errors.HASH_03
          )
      );
    }

    validateTargetFieldHeaderAttribute(
        targetField,
        group,
        configPrefix,
        "targetField",
        configIssues
    );

    validateTargetFieldHeaderAttribute(
        headerAttribute,
        group,
        configPrefix,
        "headerAttribute",
        configIssues
    );

  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> configIssues = super.init();

    if (!hasherConfig.recordHasherConfig.hashEntireRecord &&
        hasherConfig.inPlaceFieldHasherConfigs.isEmpty() &&
        hasherConfig.targetFieldHasherConfigs.isEmpty()) {
      configIssues.add(
          getContext().createConfigIssue(
              Groups.FIELD_HASHING.name(),
              "hasherConfig.targetFieldHasherConfigs",
              Errors.HASH_04
          )
      );
    }

    //Check whether the config defines combined hash
    // of fields and then check for errors in targetField.
    if (hasherConfig.recordHasherConfig.hashEntireRecord) {
      validateTarget(
          hasherConfig.recordHasherConfig.targetField,
          hasherConfig.recordHasherConfig.headerAttribute,
          Groups.RECORD_HASHING.name(),
          "hasherConfig.recordHasherConfig",
          configIssues
      );
    }

    List<TargetFieldHasherConfig> targetFieldHasherConfigs = hasherConfig.targetFieldHasherConfigs;
    for (TargetFieldHasherConfig targetFieldHasherConfig : targetFieldHasherConfigs) {
      validateTarget(
          targetFieldHasherConfig.targetField,
          targetFieldHasherConfig.headerAttribute,
          Groups.FIELD_HASHING.name(),
          "hasherConfig.targetFieldHasherConfigs",
          configIssues
      );
    }

    return configIssues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getFieldPaths();
    Set<String> fieldsDontExist = new HashSet<>();
    Set<String> fieldsWithListOrMapType = new HashSet<>();
    Set<String> fieldsWithNull = new HashSet<>();

    //Process inPlaceFieldHasherConfigs
    List<FieldHasherConfig> inPlaceFieldHasherConfigs = hasherConfig.inPlaceFieldHasherConfigs;
    processFieldHasherConfigs(record, fieldPaths, fieldsDontExist, fieldsWithListOrMapType,
        fieldsWithNull, inPlaceFieldHasherConfigs);

    //Process TargetFieldHasherConfigs
    processFieldHasherConfigs(
        record,
        fieldPaths,
        fieldsDontExist,
        fieldsWithListOrMapType,
        fieldsWithNull,
        Collections.unmodifiableList((List<? extends FieldHasherConfig>)hasherConfig.targetFieldHasherConfigs));

    //Process Record Hasher Config
    RecordHasherConfig recordHasherConfig = hasherConfig.recordHasherConfig;
    if (recordHasherConfig.hashEntireRecord) {
      handleHashingForTarget(
          record,
          recordHasherConfig.hashType,
          record.getFieldPaths(),
          fieldsDontExist,
          recordHasherConfig.targetField,
          recordHasherConfig.headerAttribute,
          recordHasherConfig.includeRecordHeaderForHashing);
    }

    if (onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR
        && !(fieldsDontExist.isEmpty() && fieldsWithListOrMapType.isEmpty() && fieldsWithNull.isEmpty())) {
      throw new OnRecordErrorException(Errors.HASH_01, record.getHeader().getSourceId(),
          Joiner.on(", ").join(fieldsDontExist),  Joiner.on(", ").join(fieldsWithNull),
          Joiner.on(", ").join(fieldsWithListOrMapType));
    }
    batchMaker.addRecord(record);
  }

  private void processFieldHasherConfigs(
      Record record,
      Set<String> fieldPaths,
      Set<String> fieldsDontExist,
      Set<String> fieldsWithListOrMapType,
      Set<String> fieldsWithNull,
      List<FieldHasherConfig> fieldHasherConfigs
  ) throws StageException {
    for (FieldHasherConfig fieldHasherConfig : fieldHasherConfigs) {
      Set<String> fieldsToHashForThisConfig = new HashSet<>();
      //Collect the matching fields to Hash.
      for (String fieldToHash : fieldHasherConfig.sourceFieldsToHash) {
        List<String> matchingFieldsPath =
            FieldRegexUtil.getMatchingFieldPaths(fieldToHash, fieldPaths);
        for (String matchingFieldPath : matchingFieldsPath) {
          if (record.has(matchingFieldPath)) {
            Field field = record.get(matchingFieldPath);
            if (field.getType() == Field.Type.MAP || field.getType() == Field.Type.LIST ||
                field.getType() == Field.Type.LIST_MAP) {
              fieldsWithListOrMapType.add(matchingFieldPath);
            } else if (field.getValue() == null) {
              fieldsWithNull.add(matchingFieldPath);
            } else {
              fieldsToHashForThisConfig.add(matchingFieldPath);
            }
          } else {
            fieldsDontExist.add(matchingFieldPath);
          }
        }
      }
      performHashingForFields(record, fieldHasherConfig, fieldsToHashForThisConfig, fieldsDontExist);
    }
  }

  private void performHashingForFields(
      Record record,
      FieldHasherConfig fieldHasherConfig,
      Set<String> fieldsToHashForThisConfig,
      Set<String> fieldsDontExist
  ) throws StageException {
    if (fieldHasherConfig instanceof TargetFieldHasherConfig) {
      TargetFieldHasherConfig targetFieldHasherConfig = ((TargetFieldHasherConfig) fieldHasherConfig);
      handleHashingForTarget(
          record,
          fieldHasherConfig.hashType,
          fieldsToHashForThisConfig,
          fieldsDontExist,
          targetFieldHasherConfig.targetField,
          targetFieldHasherConfig.headerAttribute,
          false);
    } else {
      //Perform individual one to one hashing.
      for (String fieldToHashForThisConfig : fieldsToHashForThisConfig) {
        String hashVal = generateHash(
            record,
            fieldHasherConfig.hashType,
            ImmutableList.of(fieldToHashForThisConfig),
            false);
        Field newField = Field.create(hashVal);
        record.set(fieldToHashForThisConfig, newField);
      }
    }
  }

  private void handleHashingForTarget(
      Record record,
      HashType hashType,
      Set<String> fieldsToHashForThisConfig,
      Set<String> fieldsDontExist,
      String targetField,
      String headerAttribute,
      boolean includeRecordHeader
  ) throws StageException {
    String hashVal = generateHash(record, hashType, fieldsToHashForThisConfig, includeRecordHeader);
    if (!targetField.isEmpty()) {
      Field newField = Field.create(hashVal);
      //Handle already existing field.
      if (record.has(targetField)) {
        record.set(targetField, newField);
      } else {
        record.set(targetField, newField);
        if (!record.has(targetField)) {
          fieldsDontExist.add(targetField);
        }
      }
    }
    if (!headerAttribute.isEmpty()) {
      record.getHeader().setAttribute(headerAttribute, hashVal);
    }
  }

  private String generateHash(
      Record record,
      HashType hashType,
      Collection<String> fieldsToHash,
      boolean includeRecordHeader
  ) throws StageException {
    try {
      HashFunction hasher = HashingUtil.getHasher(hashType.getDigest());
      HashingUtil.RecordFunnel recordFunnel = HashingUtil.getRecordFunnel(fieldsToHash, includeRecordHeader);
      return hasher.hashObject(record, recordFunnel).toString();
    } catch (IllegalArgumentException e) {
      throw new StageException(Errors.HASH_00, hashType.getDigest(), e.toString(), e);
    }
  }
}
