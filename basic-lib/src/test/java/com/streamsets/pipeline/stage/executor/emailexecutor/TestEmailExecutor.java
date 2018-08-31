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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestEmailExecutor {

  @Test
  public void testInitWithBadEmail() throws StageException {

    EmailConfig conf = new EmailConfig();
    conf.email = new ArrayList<>();

    conf.email.add("${badEL x}admin@example.com");

    conf.condition = "${str:contains(time:extractStringFromDate(time:now(), \"YYYY-MM-dd hh:mm:ss\"), \"20\")}";
    conf.subject = "subject ${time:extractStringFromDate(time:now(), \"yyyy\")} subject";
    conf.body = "body ${time:extractStringFromDate(time:now(), \"yyyy\")} body";

    EmailExecutor target = new EmailExecutor(ImmutableList.of(conf));
    ExecutorRunner exec = new ExecutorRunner.Builder(EmailDExecutor.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = exec.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.EMAIL_07.getCode()));

  }

  @Test
  public void testInitWithBadSubject() throws StageException {

    EmailConfig conf = new EmailConfig();
    conf.email = new ArrayList<>();
    conf.email.add("admin@exa${record:id()}mple.com");

    conf.subject = "${thisIsAnError x} subject";

    conf.condition = "${str:contains(time:extractStringFromDate(time:now(), \"YYYY-MM-dd hh:mm:ss\"), \"20\")}";
    conf.body = "body ${time:extractStringFromDate(time:now(), \"yyyy\")} body";

    EmailExecutor target = new EmailExecutor(ImmutableList.of(conf));
    ExecutorRunner exec = new ExecutorRunner.Builder(EmailDExecutor.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = exec.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.EMAIL_01.getCode()));

  }

  @Test
  public void testInitWithBadBody() throws StageException {

    EmailConfig conf = new EmailConfig();
    conf.email = new ArrayList<>();
    conf.email.add("admin@exa${record:id()}mple.com");

    conf.body = "body ${record:hahaha()x}";

    conf.condition = "${str:contains(time:extractStringFromDate(time:now(), \"YYYY-MM-dd hh:mm:ss\"), \"20\")}";
    conf.subject = "subject subject";

    EmailExecutor target = new EmailExecutor(ImmutableList.of(conf));
    ExecutorRunner exec = new ExecutorRunner.Builder(EmailDExecutor.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = exec.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.EMAIL_02.getCode()));

  }

  @Test
  public void testInitWithBadCondition() throws StageException {

    EmailConfig conf = new EmailConfig();
    conf.email = new ArrayList<>();

    conf.condition = "${total_junk x}";
    conf.subject = "subject ${time:extractStringFromDate(time:now(), \"yyyy\")} subject";
    conf.body = "body ${time:extractStringFromDate(time:now(), \"yyyy\")} body";

    EmailExecutor target = new EmailExecutor(ImmutableList.of(conf));
    ExecutorRunner exec = new ExecutorRunner.Builder(EmailDExecutor.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = exec.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.EMAIL_06.getCode()));

  }

  @Test
  public void testSendEmail() throws Exception {

    EmailConfig conf = new EmailConfig();
    conf.email = ImmutableList.of("admin@example.com");
    conf.condition = "${record:value('/condition') + 1 == 2}";
    conf.subject = "${record:value('/subject')}";
    conf.body = "${record:value('/body')}";

    ContextExtensions extensions = mock(ContextExtensions.class);
    EmailExecutor target = spy(new EmailExecutor(ImmutableList.of(conf)));
    when(target.getContextExtensions()).thenReturn(extensions);
    ExecutorRunner exec = new ExecutorRunner.Builder(
        EmailDExecutor.class,
        target
    ).setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> root = ImmutableMap.of("subject",
        Field.create("subject line"),
        "body",
        Field.create("body text"),
        "condition",
        Field.create(1)
    );

    Record conditionSatisfied = RecordCreator.create();
    conditionSatisfied.set(Field.create(root));
    Map<String, Field> unsatisfiedRoot = ImmutableMap.of("subject",
        Field.create("subject line"),
        "body",
        Field.create("body text"),
        "condition",
        Field.create(2)
    );

    Record conditionUnsatisfied = RecordCreator.create();
    conditionUnsatisfied.set(Field.create(unsatisfiedRoot));
    Map<String, Field> elErrorRoot = ImmutableMap.of("subject",
        Field.create("subject line"),
        "body",
        Field.create("body text"),
        "condition",
        Field.create("abc")
    );

    Record elError = RecordCreator.create();
    elError.set(Field.create(elErrorRoot));
    List<Record> records = ImmutableList.of(conditionSatisfied, conditionUnsatisfied, elError);
    exec.runInit();
    exec.runWrite(records);
    List<Record> errors = exec.getErrorRecords();
    Assert.assertEquals(1, errors.size());
  }
}
