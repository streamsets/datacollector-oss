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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.kudu.*;
import junit.framework.Assert;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    KuduTarget.class,
    KuduClient.class,
    KuduTable.class,
    KuduSession.class,
    Operation.class
    })
@PowerMockIgnore({ "javax.net.ssl.*" })
public class TestKuduLookup {

  private static final String KUDU_MASTER = "localhost:7051";
  private final String tableName = "test";

  @Before
  public void setup() {

    final KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

    // Create a dummy kuduSession
    PowerMockito.replace(
        MemberMatcher.method(
            KuduTarget.class,
            "openKuduSession",
            List.class
        )
    ).with(new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return client.newSession();
      }
    });

    // Sample table and schema
    List<ColumnSchema> columns = new ArrayList(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
    final Schema schema = new Schema(columns);

    // Mock KuduTable class
    KuduTable table = PowerMockito.spy(PowerMockito.mock(KuduTable.class));
    PowerMockito.stub(
        PowerMockito.method(KuduClient.class, "openTable"))
        .toReturn(table);
    PowerMockito.suppress(PowerMockito.method(KuduClient.class, "getTablesList"));
    PowerMockito.stub(PowerMockito.method(KuduClient.class, "tableExists")).toReturn(true);
    PowerMockito.when(table.getSchema()).thenReturn(schema);

    // Mock KuduSession class
    PowerMockito.suppress(PowerMockito.method(
        KuduSession.class,
        "apply",
        Operation.class
    ));
    PowerMockito.suppress(PowerMockito.method(
        KuduSession.class,
        "flush"
    ));
  }

  @Test
  public void testConnectionFailure() throws Exception{
    // Mock connection refused
    PowerMockito.stub(
        PowerMockito.method(KuduClient.class, "getTablesList"))
        .toThrow(PowerMockito.mock(KuduException.class));

    ProcessorRunner runner = setProcessorRunner(tableName);

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      Assert.assertEquals(1, issues.size());
    } catch (StageException e) {
      Assert.fail("should not throw StageException");
    }
  }

  /**
   * If table name template is not EL, we access Kudu and check if the table exists.
   * This test checks when Kudu returned true for tableExists() method.
   * @throws Exception
   */
  @Test
  public void testTableExistsNoEL() throws Exception{
    ProcessorRunner runner = setProcessorRunner(tableName);

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      Assert.assertEquals(0, issues.size());
    } catch (StageException e){
      Assert.fail();
    }
  }

  /**
   * If table name template is not EL, we access Kudu and check if the table exists.
   * This test checks when Kudu's tableExists() method returns false.
   * @throws Exception
   */
  @Test
  public void testTableDoesNotExistNoEL() throws Exception{
    // Mock table doesn't exist in Kudu.
    PowerMockito.stub(PowerMockito.method(KuduClient.class, "openTable"))
        .toThrow(PowerMockito.mock(KuduException.class));

    ProcessorRunner runner = setProcessorRunner(tableName);

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      Assert.assertEquals(1, issues.size());
    } catch (StageException e){
      Assert.fail();
    }
  }

  private ProcessorRunner setProcessorRunner(String tableName)
  {
    KuduLookupConfig conf = new KuduLookupConfig();
    conf.kuduMaster = KUDU_MASTER;
    conf.kuduTable = tableName;
    conf.fieldMappingConfigs = new ArrayList<>();
    conf.fieldMappingConfigs.add(new KuduFieldMappingConfig("/key", "key"));
    conf.fieldMappingConfigs.add(new KuduFieldMappingConfig("/value", "value"));
    KuduLookupProcessor processor = new KuduLookupProcessor(conf);
    return new ProcessorRunner.Builder(KuduLookupProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();
  }
}