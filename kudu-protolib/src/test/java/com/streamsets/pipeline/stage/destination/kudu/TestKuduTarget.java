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
package com.streamsets.pipeline.stage.destination.kudu;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;
import junit.framework.Assert;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
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
public class TestKuduTarget {

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

    // Mock Operation object behavior
    Insert insert = PowerMockito.spy(PowerMockito.mock(Insert.class));
    PowerMockito.when(table.newInsert()).thenReturn(insert);
    PowerMockito.when(insert.getRow()).thenReturn(PowerMockito.mock(PartialRow.class));

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

    TargetRunner targetRunner = setTargetRunner(tableName, KuduOperationType.INSERT, UnsupportedOperationAction.DISCARD);

    try {
      List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
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
    TargetRunner targetRunner = setTargetRunner(tableName, KuduOperationType.INSERT, UnsupportedOperationAction.DISCARD);

    try {
      List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
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
    PowerMockito.stub(PowerMockito.method(KuduClient.class, "tableExists")).toReturn(false);

    TargetRunner targetRunner = setTargetRunner(tableName, KuduOperationType.INSERT, UnsupportedOperationAction.DISCARD);

    try {
      List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
      Assert.assertEquals(1, issues.size());
    } catch (StageException e){
      Assert.fail();
    }
  }

  /**
   * When table name template contains EL, it checks if the table exists for
   * every single records. This test checks no error when tableExists() returned false.
   * @throws Exception
   */
  @Test
  public void testTableExistWithEL() throws Exception{
    TargetRunner targetRunner = setTargetRunner(
        "${record:attribute('tableName')}",
        KuduOperationType.INSERT,
        UnsupportedOperationAction.DISCARD
    );

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("key", Field.create(1));
    field.put("value", Field.create("value"));
    field.put("name", Field.create("name"));
    record.set(Field.createListMap(field));
    record.getHeader().setAttribute("tableName", "test_table");
    targetRunner.runInit();

    try {
      targetRunner.runWrite(ImmutableList.of(record));
    } catch (StageException e){
      Assert.fail();
    }
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    targetRunner.runDestroy();
  }

  /**
   * Ensure that if given field is null and column doesn't support that, the record will
   * end up in error stream rather then terminating whole pipeline execution.
   */
  @Test
  public void testNullColumnWillEndInError() throws Exception{
    TargetRunner targetRunner = setTargetRunner(tableName, KuduOperationType.INSERT, UnsupportedOperationAction.SEND_TO_ERROR);
    targetRunner.runInit();

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("key", Field.create(1));
    field.put("value", Field.create(Field.Type.STRING, null));
    field.put("name", Field.create(Field.Type.STRING, null));
    record.set(Field.createListMap(field));

    try {
      targetRunner.runWrite(ImmutableList.of(record));

      List<Record> errors = targetRunner.getErrorRecords();
      Assert.assertEquals(1, errors.size());
    } finally {
      targetRunner.runDestroy();
    }
  }

  /**
   * Checks that a LineageEvent is returned.
   * @throws Exception
   */
  @Test
  public void testLineageEvent() throws Exception{
    TargetRunner targetRunner = setTargetRunner(
        "${record:attribute('tableName')}",
        KuduOperationType.INSERT,
        UnsupportedOperationAction.DISCARD
    );

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("key", Field.create(1));
    field.put("value", Field.create("value"));
    field.put("name", Field.create("name"));
    record.set(Field.createListMap(field));
    record.getHeader().setAttribute("tableName", "test_table");
    targetRunner.runInit();

    try {
      targetRunner.runWrite(ImmutableList.of(record));
    } catch (StageException e){
      Assert.fail();
    }
    List<LineageEvent> events = targetRunner.getLineageEvents();
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(LineageEventType.ENTITY_WRITTEN, events.get(0).getEventType());
    Assert.assertEquals(
        "test_table",
        events.get(0).getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME)
    );
    targetRunner.runDestroy();
  }

  /**
   * When tableNameTemplate contains EL, it checks if the table exists in Kudu
   * for every single records. This test checks error records when the table
   * does not exist in Kudu.
   * @throws Exception
   */
  @Test
  public void testTableDoesNotExistWithEL() throws Exception{
    // Mock KuduClient.openTable() to throw KuduException
    PowerMockito.stub(PowerMockito.method(KuduClient.class, "openTable")).toThrow(PowerMockito.mock(KuduException.class));

    TargetRunner targetRunner = setTargetRunner(
        "${record:attribute('tableName')}",
        KuduOperationType.INSERT,
        UnsupportedOperationAction.DISCARD
    );

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("key", Field.create(1));
    record.set(Field.createListMap(field));
    record.getHeader().setAttribute("tableName", "do_not_exist");
    targetRunner.runInit();

    try {
      targetRunner.runWrite(Collections.singletonList(record));
      Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    } catch (StageException e){
      Assert.fail();
    }
    targetRunner.runDestroy();
  }


  /**
   * Operation code in record header must be a numeric value. For example.
   * 1: insert, 2: update, 3: delete
   * This is to test operation code is not numeric and the record is handled
   * by UnsupportedOperationAction.
   * @throws Exception
   */
  @Test
  public void testInvalidOperationInHeaderDiscard() throws Exception {
    TargetRunner targetRunner = setTargetRunner(
        tableName,
        KuduOperationType.INSERT,
        UnsupportedOperationAction.DISCARD
    );

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("a", Field.create("test"));
    record.set(Field.createListMap(field));
    // this header causes NumberFormatException, and this record should be discarded
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, "random");
    List<Record> test = Collections.singletonList(record);
    targetRunner.runInit();

    try {
      targetRunner.runWrite(test);
    } catch (StageException e){
      Assert.fail();
    }
    Assert.assertEquals(0, targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }

  @Test
  public void testInvalidOperationInHeaderSendError() throws Exception {

    TargetRunner targetRunner = setTargetRunner(
        tableName,
        KuduOperationType.INSERT,
        UnsupportedOperationAction.SEND_TO_ERROR
    );

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("a", Field.create("test"));
    record.set(Field.createListMap(field));
    // this header causes NumberFormatException, and this record should be sent to error
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, "random");
    targetRunner.runInit();

    try {
      targetRunner.runWrite(Collections.singletonList(record));
    } catch (StageException e){
      Assert.fail();
    }
    Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }


  @Test
  public void testInvalidOperationInHeaderUseDefault() throws Exception {

    TargetRunner targetRunner = setTargetRunner(
        tableName,
        KuduOperationType.INSERT,
        UnsupportedOperationAction.USE_DEFAULT
    );

    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("key", Field.create(1));
    field.put("value", Field.create("value"));
    field.put("name", Field.create("name"));
    record.set(Field.createListMap(field));
    // this header causes NumberFormatException, and should be defaulted to insert
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, "random");
    targetRunner.runInit();

    try {
      targetRunner.runWrite(Collections.singletonList(record));
    } catch (StageException e){
      Assert.fail();
    }
    Assert.assertEquals(0, targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }

  @Test
  public void testUnsupportedOperationInRecordHeader() throws Exception {

    TargetRunner targetRunner = setTargetRunner(
        tableName,
        KuduOperationType.INSERT,
        UnsupportedOperationAction.DISCARD
    );
    Record record =  RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("key", Field.create(1));
    field.put("value", Field.create("value"));
    field.put("name", Field.create("name"));
    record.set(Field.createListMap(field));
    // UNSUPPORTED_CODE is not supported by Kudu. This record gets discarded
    record.getHeader().setAttribute(
        OperationType.SDC_OPERATION_TYPE,
        String.valueOf(OperationType.UNSUPPORTED_CODE)
    );
    targetRunner.runInit();

    try {
      targetRunner.runWrite(Collections.singletonList(record));
    } catch (StageException e){
      Assert.fail();
    }
    Assert.assertEquals(0, targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }


  private TargetRunner setTargetRunner(String tableName,
                                       KuduOperationType defaultOperation,
                                       UnsupportedOperationAction action)
  {
    KuduTarget target = new KuduTarget(new KuduConfigBeanBuilder()
        .setMaster(KUDU_MASTER)
        .setTableName(tableName)
        .setDefaultOperation(defaultOperation)
        .setUnsupportedAction(action)
        .build());

    return new TargetRunner.Builder(KuduDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
  }

  public class KuduConfigBeanBuilder {

    String kuduMaster;
    String tableName;
    KuduOperationType defaultOperation;
    List<KuduFieldMappingConfig> mapping;
    UnsupportedOperationAction unsupportedAction;

    public KuduConfigBeanBuilder setMaster(String master) {
      this.kuduMaster = master;
      return this;
    }

    public KuduConfigBeanBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public KuduConfigBeanBuilder setDefaultOperation(KuduOperationType type){
      this.defaultOperation = type;
      return this;
    }

    public KuduConfigBeanBuilder setUnsupportedAction(UnsupportedOperationAction action) {
      this.unsupportedAction = action;
      return this;
    }

    public KuduConfigBean build() {
      KuduConfigBean conf = new KuduConfigBean();
      conf.kuduMaster = kuduMaster;
      conf.tableNameTemplate = tableName;
      conf.defaultOperation = KuduOperationType.INSERT;
      conf.fieldMappingConfigs = mapping;
      conf.unsupportedAction = unsupportedAction;
      return conf;
    }
  }
}
