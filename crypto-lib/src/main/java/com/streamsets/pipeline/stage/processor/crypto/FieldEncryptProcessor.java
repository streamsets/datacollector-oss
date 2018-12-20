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
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.crypto.CryptoErrors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class FieldEncryptProcessor extends SingleLaneRecordProcessor {
  private final FieldEncrypter encrypter;
  private ProcessorFieldEncryptConfig conf;
  private BiFunction<Field, Map<String, String>, byte[]> prepare;
  private BiFunction<Record, Field, Optional<Field>> checkInput;
  private Function<CryptoResult<byte[], ?>, Field> createResultField;

  public FieldEncryptProcessor(ProcessorFieldEncryptConfig conf) {
    this.conf = conf;
    this.encrypter = new FieldEncrypter(conf, conf.mode);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    encrypter.setContext(getContext());
    encrypter.init(issues);

    if (conf.mode == EncryptionMode.ENCRYPT) {
      prepare = encrypter::prepareEncrypt;
      checkInput = encrypter::checkInputEncrypt;
      createResultField = encrypter::createResultFieldEncrypt;
    } else {
      prepare = (field, context) -> encrypter.prepareDecrypt(field);
      checkInput = encrypter::checkInputDecrypt;
      createResultField = encrypter::createResultFieldDecrypt;
    }

    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Map<String, String> encryptionContext = new HashMap<>(conf.context);

    try {
      for (String fieldPath : conf.fieldPaths) {

        Field field = record.get(fieldPath);

        // reviewer requested no use of Java 8 streams
        if (field != null && field.getValue() != null) { // process if field is present and non-null

          Optional<Field> input = checkInput.apply(record, field);

          if (input.isPresent()) {
            byte[] bytes = prepare.apply(input.get(), encryptionContext);
            CryptoResult<byte[], ?> result = encrypter.process(bytes, encryptionContext);
            field = createResultField.apply(result);
            record.set(fieldPath, field);
          } else {
            return; // record sent to error, done with this record.
          }
        }
      }
    // The encryption process can throw a lot of unchecked exceptions that if not caught would terminate the pipeline
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, StageException.class);
      throw new StageException(CryptoErrors.CRYPTO_07, e.toString(), e);
    }

    singleLaneBatchMaker.addRecord(record);
  }
}
