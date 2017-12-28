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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.KuduException;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    KuduLookupProcessor.class,
    AsyncKuduClient.class,
    KuduTable.class,
    AsyncKuduSession.class,
    })
@PowerMockIgnore({ "javax.net.ssl.*" })
public class TestKuduLookup {

  private static final String KUDU_MASTER = "localhost:7051";
  private final String tableName = "test";

  @Before
  public void setup() {
    // Sample table and schema
    List<ColumnSchema> columns = new ArrayList(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
    final Schema schema = new Schema(columns);

    // Mock KuduTable class
    KuduTable table = PowerMockito.spy(PowerMockito.mock(KuduTable.class));
    PowerMockito.suppress(PowerMockito.method(AsyncKuduClient.class, "getTablesList"));
    PowerMockito.when(table.getSchema()).thenReturn(schema);

    // Mock KuduSession class
    PowerMockito.suppress(PowerMockito.method(
        AsyncKuduSession.class,
        "apply",
        Operation.class
    ));
    PowerMockito.suppress(PowerMockito.method(
        AsyncKuduSession.class,
        "flush"
    ));
    PowerMockito.suppress(PowerMockito.method(
        KuduLookupProcessor.class,
        "destroy"
    ));
  }

  @Test
  public void testConnectionFailure() throws Exception{
    // Mock connection refused
    PowerMockito.stub(
        PowerMockito.method(AsyncKuduClient.class, "getTablesList"))
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
   * This test checks when Kudu's tableExists() method returns false.
   * @throws Exception
   */
  @Test
  public void testTableDoesNotExistNoEL() throws Exception{
    // Mock table doesn't exist in Kudu.
    PowerMockito.stub(
        PowerMockito.method(AsyncKuduClient.class, "tableExists"))
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
    conf.kuduTableTemplate = tableName;
    conf.keyColumnMapping = new ArrayList<>();
    conf.keyColumnMapping.add(new KuduFieldMappingConfig("/key", "key"));
    conf.outputColumnMapping = new ArrayList<>();
    conf.outputColumnMapping.add(new KuduOutputColumnMapping("column", "/field", ""));
    KuduLookupProcessor processor = new KuduLookupProcessor(conf);
    return new ProcessorRunner.Builder(KuduLookupDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();
  }

}
