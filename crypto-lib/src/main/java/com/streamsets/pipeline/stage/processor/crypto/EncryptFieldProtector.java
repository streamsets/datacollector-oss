/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.crypto;

import com.amazonaws.encryptionsdk.CryptoResult;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldBatch;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseFieldProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

@StageDef(
    version = 1,
    label = "Encrypt Data",
    description = "Encrypt Fields",
    upgraderDef = "upgrader/EncryptFieldProtector.yaml",
    onlineHelpRefUrl = "index.html?contextID=concept_w3j_24j_cgb" 
)
@ConfigGroups(ProtectorEncryptGroups.class)
@HideStage(HideStage.Type.FIELD_PROCESSOR)
@HideConfigs(preconditions = true, onErrorRecord = true)
public class EncryptFieldProtector extends BaseFieldProcessor {
  @ConfigDefBean
  public ProtectorFieldEncryptConfig conf = new ProtectorFieldEncryptConfig();

  private FieldEncrypter encrypter;
  private BiFunction<Field, Map<String, String>, byte[]> prepare;
  private Function<CryptoResult<byte[], ?>, Field> createResultField;

  public EncryptFieldProtector() {
    super();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    encrypter = new FieldEncrypter(conf, EncryptionMode.ENCRYPT);
    encrypter.setContext(getContext());
    encrypter.init(issues);

    prepare = encrypter::prepareEncrypt;
    createResultField = encrypter::createResultFieldEncrypt;

    return issues;
  }

  @Override
  public void process(FieldBatch batch) throws StageException {
    Map<String, String> encryptionContext = new HashMap<>(conf.context);

    while(batch.next()) {
      Field field = batch.getField();
      // reviewer requested no use of Java 8 streams
      if (field != null && field.getValue() != null) { // process if field is present and non-null

        Optional<Field> input = encrypter.checkInputEncrypt(field);

        if (input.isPresent()) {
          byte[] bytes = prepare.apply(input.get(), encryptionContext);
          CryptoResult<byte[], ?> result = encrypter.process(bytes, encryptionContext);
          Field encryptedField = createResultField.apply(result);
          batch.replace(encryptedField);
        } else {
          return; // record sent to error, done with this record.
        }
      }
    }
  }
}
