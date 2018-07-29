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
package com.streamsets.pipeline.destination.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class AerospikeTarget extends RecordTarget {
  private static final Logger LOG = LoggerFactory.getLogger(AerospikeTarget.class);
  // config names
  private static final String NAMESPACE = "namespaceEL";
  private static final String SET = "setEL";
  private static final String KEY = "keyEL";
  private static final String BIN_CONFIG_LIST = "binConfigsEL";


  private AerospikeTargetELEvals elEvals = new AerospikeTargetELEvals();
  private final WritePolicy defaultWritePolicy = new WritePolicy();
  private final AerospikeBeanConfig aerospikeBeanConfig;
  private final String namespaceEL;
  private final String setEL;
  private final String keyEL;
  private List<BinConfig> binConfigList;


  private static class AerospikeTargetELEvals {
    private ELEval namespaceELEval;
    private ELEval setELEval;
    private ELEval keyELEval;


    public void init(Target.Context context) {
      namespaceELEval = context.createELEval(NAMESPACE);
      setELEval = context.createELEval(SET);
      keyELEval = context.createELEval(KEY);
    }
  }

  public AerospikeTarget(AerospikeBeanConfig aerospikeBeanConfig, String namespaceEL, String setEL, String keyEL, List<BinConfig> binConfigEL) {
    this.aerospikeBeanConfig = aerospikeBeanConfig;
    this.namespaceEL = namespaceEL;
    this.setEL = setEL;
    this.keyEL = keyEL;
    this.binConfigList = binConfigEL;
  }


  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    // initialized and validated client
    aerospikeBeanConfig.init(getContext(), issues);

    elEvals.init(getContext());

    // validate bins
    if (binConfigList.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.MAPPING.getLabel(), BIN_CONFIG_LIST, AerospikeErrors.AEROSPIKE_05));
    } else {
      binConfigList.forEach(binConfig -> {
        validateRequiredEL(getContext(), binConfig.binName, issues, "binName", String.class);
        validateRequiredEL(getContext(), binConfig.binValue, issues, "binValue", binConfig.valueType.getClassName());
      });


    }

    // validate namespace, key and set
    validateEL(getContext(), elEvals.namespaceELEval, namespaceEL, Groups.MAPPING.getLabel(),
        AerospikeErrors.AEROSPIKE_07, issues, String.class);
    validateEL(getContext(), elEvals.keyELEval, keyEL, Groups.MAPPING.getLabel(),
        AerospikeErrors.AEROSPIKE_07, issues, String.class);
    validateEL(getContext(), elEvals.setELEval, setEL, Groups.MAPPING.getLabel(),
        AerospikeErrors.AEROSPIKE_07, issues, String.class);
    return issues;
  }

  @Override
  protected void write(Record record) throws StageException {
    ELVars variables = getContext().createELVars();
    RecordEL.setRecordInContext(variables, record);
    TimeEL.setCalendarInContext(variables, Calendar.getInstance());
    TimeNowEL.setTimeNowInContext(variables, new Date());

    List<Bin> bins = new LinkedList<>();
    for (BinConfig binConfig : binConfigList) {
      bins.add(
          new Bin(
              resolveEL(getContext().createELEval("binName"), variables, binConfig.binName, String.class),
              resolveEL(getContext().createELEval("binValue"), variables, binConfig.binValue, binConfig.valueType.getClassName())
          )
      );
    }
    String namespace = resolveEL(elEvals.namespaceELEval, variables, namespaceEL, String.class);
    String set = resolveEL(elEvals.setELEval, variables, setEL, String.class);
    String key = resolveEL(elEvals.keyELEval, variables, keyEL, String.class);

    Key k = new Key(namespace, set, key);
    int retryCount = 0;
    while (retryCount <= aerospikeBeanConfig.maxRetries) {
      try {
        aerospikeBeanConfig.getAerospikeClient().put(
            defaultWritePolicy,
            k,
            bins.toArray(new Bin[bins.size()])
        );
        return;
      } catch (AerospikeException e) {
        retryCount++;
        if (retryCount  > aerospikeBeanConfig.maxRetries) {
          throw new OnRecordErrorException(AerospikeErrors.AEROSPIKE_04, e);
        }
      }
    }

  }

  @Override
  public void destroy() {
    aerospikeBeanConfig.destroy();
    super.destroy();

  }


  private static void validateEL(Target.Context context, ELEval elEval, String elStr, String group, ErrorCode evalError,
                                 List<ConfigIssue> issues, Class className) {
    ELVars vars = context.createELVars();
    RecordEL.setRecordInContext(vars, context.createRecord("validateConfigs"));
    TimeEL.setCalendarInContext(vars, Calendar.getInstance());
    try {
      elEval.eval(vars, elStr, className);
    } catch (ELEvalException ex) {
      issues.add(context.createConfigIssue(group, elEval.getConfigName(), evalError, elStr, ex));
      ConfigIssue x = context.createConfigIssue(group, elEval.getConfigName(), evalError, elStr, ex);
    }
  }

  private static void validateRequiredEL(Target.Context context, String value, List<ConfigIssue> issues, String configName, Class className) {
    if (value.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.MAPPING.getLabel(), configName, AerospikeErrors.AEROSPIKE_01));
    } else {
      validateEL(context, context.createELEval(configName), value, Groups.MAPPING.getLabel(), AerospikeErrors.AEROSPIKE_07, issues, className);
    }
  }

  private static <T> T resolveEL(ELEval elEval, ELVars elVars, String configValue, Class<T> returnType) throws ELEvalException {
    return elEval.eval(elVars, configValue, returnType);
  }

}
