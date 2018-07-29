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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.opcua.server.ExampleServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestOpcUaClientSource {
  private static ExampleServer exampleServer;

  @BeforeClass
  public static void setUp() throws Exception {
    // exampleServer = new ExampleServer();
    // exampleServer.startup().get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // exampleServer.shutdown();
  }

  private OpcUaClientSourceConfigBean getConfig(OpcUaReadMode readMode, List<NodeIdConfig> nodeIdConfigs) {
    OpcUaClientSourceConfigBean conf = new OpcUaClientSourceConfigBean();
    conf.resourceUrl = "opc.tcp://localhost:12686/example";
    conf.readMode = readMode;
    conf.nodeIdConfigs = nodeIdConfigs;
    return conf;
  }


  @Test
  public void testInvalidOpcUaURL() throws Exception {
    OpcUaClientSourceConfigBean conf = new OpcUaClientSourceConfigBean();
    conf.resourceUrl = "opc.tcp://localhost:12323/invalid";
    conf.readMode = OpcUaReadMode.BROWSE_NODES;
    conf.nodeIdConfigs = Collections.emptyList();
    OpcUaClientSource source = new OpcUaClientSource(conf);
    PushSourceRunner runner = new PushSourceRunner
        .Builder(OpcUaClientDSource.class, source)
        .addOutputLane("a").build();
    try {
      runner.runInit();
      Assert.assertTrue(false);
    } catch (StageException ex){
      Assert.assertNotNull(ex.getErrorCode());
      Assert.assertEquals(ex.getErrorCode().getCode(), "CONTAINER_0010");
    }
  }

  @Test
  @Ignore
  public void testBrowseDescReadMode() throws Exception {
    OpcUaClientSource source = new OpcUaClientSource(getConfig(OpcUaReadMode.BROWSE_NODES, Collections.emptyList()));
    PushSourceRunner runner = new PushSourceRunner
        .Builder(OpcUaClientDSource.class, source)
        .addOutputLane("a").build();

    runner.runInit();
    try {
      List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      runner.waitOnProduce();
      Assert.assertEquals(1, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  @Ignore
  public void testPollingReadMode() throws Exception {
    OpcUaClientSource source = new OpcUaClientSource(getConfig(OpcUaReadMode.POLLING, getTestNodeConfigs()));
    PushSourceRunner runner = new PushSourceRunner
        .Builder(OpcUaClientDSource.class, source)
        .addOutputLane("a").build();

    runner.runInit();
    try {
      List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      Assert.assertTrue(runner.getErrors().isEmpty());
      runner.waitOnProduce();
      Assert.assertEquals(1, records.size());
      Assert.assertNotNull(records.get(0).get("/Boolean").getValue());
      Assert.assertNull(records.get(0).get("/OnlyAdminCanRead").getValue());
      Assert.assertNotNull(records.get(0).get("/BooleanArray").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  @Ignore
  public void testSubscribeReadMode() throws Exception {
    List<NodeIdConfig> nodeIdConfigs = new ArrayList<>();
    NodeIdConfig nodeIdConfig = new NodeIdConfig();
    nodeIdConfig.field = "helloWorldDynamicBoolean";
    nodeIdConfig.identifierType = IdentifierType.STRING;
    nodeIdConfig.namespaceIndex = 2;
    nodeIdConfig.identifier = "HelloWorld/Dynamic/Boolean";
    nodeIdConfigs.add(nodeIdConfig);

    OpcUaClientSource source = new OpcUaClientSource(getConfig(OpcUaReadMode.SUBSCRIBE, nodeIdConfigs));
    PushSourceRunner runner = new PushSourceRunner
        .Builder(OpcUaClientDSource.class, source)
        .addOutputLane("a").build();

    runner.runInit();
    try {
      List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      Assert.assertTrue(runner.getErrors().isEmpty());
      runner.waitOnProduce();
      Assert.assertEquals(1, records.size());
      Assert.assertNotNull(records.get(0).get("/helloWorldDynamicBoolean").getValue());
    } finally {
      runner.runDestroy();
    }
  }


  private List<NodeIdConfig> getTestNodeConfigs() {
    List<NodeIdConfig> nodeIdConfigs = new ArrayList<>();
    nodeIdConfigs.add(getNodeIdConfig("Boolean", "HelloWorld/ScalarTypes/Boolean"));
    nodeIdConfigs.add(getNodeIdConfig("Byte", "HelloWorld/ScalarTypes/Byte"));
    nodeIdConfigs.add(getNodeIdConfig("SByte", "HelloWorld/ScalarTypes/SByte"));
    nodeIdConfigs.add(getNodeIdConfig("Int16", "HelloWorld/ScalarTypes/Int16"));
    nodeIdConfigs.add(getNodeIdConfig("Int32", "HelloWorld/ScalarTypes/Int32"));
    nodeIdConfigs.add(getNodeIdConfig("Int64", "HelloWorld/ScalarTypes/Int64"));
    nodeIdConfigs.add(getNodeIdConfig("UInt16", "HelloWorld/ScalarTypes/UInt16"));
    nodeIdConfigs.add(getNodeIdConfig("UInt32", "HelloWorld/ScalarTypes/UInt32"));
    nodeIdConfigs.add(getNodeIdConfig("UInt64", "HelloWorld/ScalarTypes/UInt64"));
    nodeIdConfigs.add(getNodeIdConfig("Float", "HelloWorld/ScalarTypes/Float"));
    nodeIdConfigs.add(getNodeIdConfig("Double", "HelloWorld/ScalarTypes/Double"));
    nodeIdConfigs.add(getNodeIdConfig("String", "HelloWorld/ScalarTypes/String"));
    nodeIdConfigs.add(getNodeIdConfig("DateTime", "HelloWorld/ScalarTypes/DateTime"));
    nodeIdConfigs.add(getNodeIdConfig("Guid", "HelloWorld/ScalarTypes/Guid"));
    nodeIdConfigs.add(getNodeIdConfig("ByteString", "HelloWorld/ScalarTypes/ByteString"));
    nodeIdConfigs.add(getNodeIdConfig("XmlElement", "HelloWorld/ScalarTypes/XmlElement"));
    nodeIdConfigs.add(getNodeIdConfig("LocalizedText", "HelloWorld/ScalarTypes/LocalizedText"));
    nodeIdConfigs.add(getNodeIdConfig("NodeId", "HelloWorld/ScalarTypes/NodeId"));
    nodeIdConfigs.add(getNodeIdConfig("Duration", "HelloWorld/ScalarTypes/Duration"));
    nodeIdConfigs.add(getNodeIdConfig("UtcTime", "HelloWorld/ScalarTypes/UtcTime"));

    nodeIdConfigs.add(getNodeIdConfig("OnlyAdminCanRead", "HelloWorld/OnlyAdminCanRead"));


    nodeIdConfigs.add(getNodeIdConfig("BooleanArray", "HelloWorld/ArrayTypes/BooleanArray"));
    nodeIdConfigs.add(getNodeIdConfig("ByteArray", "HelloWorld/ArrayTypes/ByteArray"));
    nodeIdConfigs.add(getNodeIdConfig("SByteArray", "HelloWorld/ArrayTypes/SByteArray"));
    nodeIdConfigs.add(getNodeIdConfig("Int16Array", "HelloWorld/ArrayTypes/Int16Array"));
    nodeIdConfigs.add(getNodeIdConfig("Int32Array", "HelloWorld/ArrayTypes/Int32Array"));
    nodeIdConfigs.add(getNodeIdConfig("Int64Array", "HelloWorld/ArrayTypes/Int64Array"));
    nodeIdConfigs.add(getNodeIdConfig("UInt16Array", "HelloWorld/ArrayTypes/UInt16Array"));
    nodeIdConfigs.add(getNodeIdConfig("UInt32Array", "HelloWorld/ArrayTypes/UInt32Array"));
    nodeIdConfigs.add(getNodeIdConfig("UInt64Array", "HelloWorld/ArrayTypes/UInt64Array"));
    nodeIdConfigs.add(getNodeIdConfig("FloatArray", "HelloWorld/ArrayTypes/FloatArray"));
    nodeIdConfigs.add(getNodeIdConfig("DoubleArray", "HelloWorld/ArrayTypes/DoubleArray"));
    nodeIdConfigs.add(getNodeIdConfig("StringArray", "HelloWorld/ArrayTypes/StringArray"));
    nodeIdConfigs.add(getNodeIdConfig("DateTimeArray", "HelloWorld/ArrayTypes/DateTimeArray"));
    nodeIdConfigs.add(getNodeIdConfig("GuidArray", "HelloWorld/ArrayTypes/GuidArray"));
    nodeIdConfigs.add(getNodeIdConfig("ByteStringArray", "HelloWorld/ArrayTypes/ByteStringArray"));
    nodeIdConfigs.add(getNodeIdConfig("XmlElementArray", "HelloWorld/ArrayTypes/XmlElementArray"));
    nodeIdConfigs.add(getNodeIdConfig("LocalizedTextArray", "HelloWorld/ArrayTypes/LocalizedTextArray"));
    nodeIdConfigs.add(getNodeIdConfig("NodeIdArray", "HelloWorld/ArrayTypes/NodeIdArray"));
    nodeIdConfigs.add(getNodeIdConfig("DurationArray", "HelloWorld/ArrayTypes/DurationArray"));
    nodeIdConfigs.add(getNodeIdConfig("UtcTimeArray", "HelloWorld/ArrayTypes/UtcTimeArray"));
    return nodeIdConfigs;
  }

  private NodeIdConfig getNodeIdConfig(String fieldName, String identifier) {
    NodeIdConfig nodeIdConfig = new NodeIdConfig();
    nodeIdConfig.field = fieldName;
    nodeIdConfig.identifierType = IdentifierType.STRING;
    nodeIdConfig.namespaceIndex = 2;
    nodeIdConfig.identifier = identifier;
    return nodeIdConfig;
  }
}
