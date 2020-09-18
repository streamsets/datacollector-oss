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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(InfluxTarget.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestInfluxTarget {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testInfluxTarget() throws Exception {
    InfluxTarget target = PowerMockito.spy(new InfluxTarget(getConfigBean()));
    TargetRunner runner = new TargetRunner.Builder(InfluxDTarget.class, target).build();

    InfluxDB client = mock(InfluxDB.class);
    when(client.describeDatabases()).thenReturn(ImmutableList.of("test"));

    PowerMockito.doReturn(client).when(target, "getClient", any(InfluxConfigBean.class));

    runner.runInit();

    List<Record> records = getRecords();
    runner.runWrite(records);

    verify(client, times(1)).write(any(BatchPoints.class));

    runner.runDestroy();
  }

  @Test
  public void testNoAutoCreateMissingDb() throws Exception {
    InfluxConfigBean conf = getConfigBean();
    conf.autoCreate = false;
    conf.dbName = "abcdef";

    InfluxTarget target = PowerMockito.spy(new InfluxTarget(conf));
    TargetRunner runner = new TargetRunner.Builder(InfluxDTarget.class, target).build();

    InfluxDB client = mock(InfluxDB.class);
    when(client.describeDatabases()).thenReturn(ImmutableList.of("test"));

    PowerMockito.doReturn(client).when(target, "getClient", any(InfluxConfigBean.class));

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testAutoCreateMissingDb() throws Exception {
    InfluxConfigBean conf = getConfigBean();
    conf.autoCreate = true;
    conf.dbName = "abcdef";

    InfluxTarget target = PowerMockito.spy(new InfluxTarget(conf));
    TargetRunner runner = new TargetRunner.Builder(InfluxDTarget.class, target).build();

    InfluxDB client = mock(InfluxDB.class);
    when(client.describeDatabases()).thenReturn(ImmutableList.of("test"));

    PowerMockito.doReturn(client).when(target, "getClient", any(InfluxConfigBean.class));

    runner.runInit();
    verify(client, times(1)).createDatabase(conf.dbName);
  }

  private List<Record> getRecords() {
    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();

    Map<String, Field> recordValue = new HashMap<>();
    recordValue.put("host", Field.create(TestCollectdRecordConverter.HOSTNAME));
    recordValue.put("plugin", Field.create(TestCollectdRecordConverter.PLUGIN));
    recordValue.put("plugin_instance", Field.create(TestCollectdRecordConverter.PLUGIN_INSTANCE));
    recordValue.put("type", Field.create(TestCollectdRecordConverter.TYPE));
    recordValue.put("rx", Field.create(TestCollectdRecordConverter.OCTETS_RX));
    recordValue.put("time", Field.create(TestCollectdRecordConverter.TIME_MILLIS));

    record.set(Field.create(recordValue));
    records.add(record);

    return records;
  }

  private InfluxConfigBean getConfigBean() {
    InfluxConfigBean conf = new InfluxConfigBean();
    conf.url = "http://localhost:8086";
    conf.dbName = "test";
    conf.username = () -> "test";
    conf.password = () -> "test";
    conf.recordConverterType = RecordConverterType.COLLECTD;
    return conf;
  }
}
