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
package com.streamsets.pipeline.stage.executor.emailexecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EmailExecutor extends BaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(EmailExecutor.class);
  private ErrorRecordHandler errorRecordHandler;

  private List<EmailConfig> conf;

  private Set<String> recipients = new HashSet<>();

  EmailExecutor(List<EmailConfig> conf) {
    this.conf = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    List<ConfigIssue> issues = super.init();

    for (EmailConfig co : conf) {
      co.subjectELEval = getContext().createELEval("subject");
      co.subjectELVars = getContext().createELVars();
      ELUtils.validateExpression(co.subject,
          getContext(),
          Groups.EMAILS.name(),
          co.subject,
          Errors.EMAIL_01, issues
      );

      co.bodyELEval = getContext().createELEval("body");
      co.bodyELVars = getContext().createELVars();
      ELUtils.validateExpression(co.body,
          getContext(),
          Groups.EMAILS.name(),
          co.body,
          Errors.EMAIL_02, issues
      );

      co.conditionELEval = getContext().createELEval("condition");
      co.conditionELVars = getContext().createELVars();
      ELUtils.validateExpression(co.condition,
          getContext(),
          Groups.EMAILS.name(),
          co.condition,
          Errors.EMAIL_06, issues
      );

      co.emailELEval = new HashMap<>();
      co.emailELVars = new HashMap<>();
      for (String str : co.email) {
        ELEval eval = getContext().createELEval("email");
        ELVars vars = getContext().createELVars();
        ELUtils.validateExpression(str,
            getContext(),
            Groups.EMAILS.name(),
            str,
            Errors.EMAIL_07, issues
        );
        co.emailELEval.put(str, eval);
        co.emailELVars.put(str, vars);
      }
    }

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {

    Iterator<Record> iter = batch.getRecords();
    while (iter.hasNext()) {
      Record rec = iter.next();
      try {
        // for each configuration group in the UI...
        for (EmailConfig co : conf) {
          if (!co.condition.isEmpty() && !processAnyEls(co.conditionELEval,
              co.conditionELVars,
              co.condition,
              Boolean.class,
              rec,
              Errors.EMAIL_03,
              "condition"
          )) {
            continue;
          }

          String subject = processAnyEls(co.subjectELEval,
              co.subjectELVars,
              co.subject,
              String.class,
              rec,
              Errors.EMAIL_03,
              "subject"
          );

          String body = processAnyEls(co.bodyELEval,
              co.bodyELVars,
              co.body,
              String.class,
              rec,
              Errors.EMAIL_03,
              "body"
          );

          recipients.clear();
          for (String email : co.email) {
            String address;
            address = processAnyEls(co.emailELEval.get(email),
                co.emailELVars.get(email),
                email,
                String.class,
                rec,
                Errors.EMAIL_03,
                "address"
            );
            recipients.add(address);
          }

          if (!recipients.isEmpty()) {
            getContextExtensions().notify(Lists.newArrayList(recipients), subject, body);
          }
        }

      } catch (OnRecordErrorException ex) {
        LOG.error(Errors.EMAIL_05.getMessage(), ex.getMessage());
        errorRecordHandler.onError(ex);
      }
    }
  }

  private <T> T processAnyEls(
      ELEval expressionVals,
      ELVars expressionVars,
      String text,
      Class<T> returnType,
      Record rec,
      Errors err,
      String field
  ) throws OnRecordErrorException {

    try {
      RecordEL.setRecordInContext(expressionVars, rec);
      return expressionVals.eval(expressionVars, text, returnType);

    } catch (ELEvalException ex) {
      LOG.error(err.getMessage(), text, field, ex);
      throw new OnRecordErrorException(rec, err, text, field, ex);
    }
  }

  @VisibleForTesting
  ContextExtensions getContextExtensions() {
    return (ContextExtensions) getContext();
  }
}
