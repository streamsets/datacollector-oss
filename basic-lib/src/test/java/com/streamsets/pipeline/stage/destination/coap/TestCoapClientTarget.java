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
package com.streamsets.pipeline.stage.destination.coap;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.http.TestHttpClientTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestCoapClientTarget {
  private CoapServer coapServer;
  private static boolean serverRequested = false;
  private static String requestPayload = null;
  private static boolean returnErrorResponse = false;
  private static String resourceURl = null;

  @Before
  public void setUp() throws Exception {
    int port =  TestHttpClientTarget.getFreePort();
    coapServer = new CoapServer(NetworkConfig.createStandardWithoutFile(), port);
    coapServer.add(new CoapResource("test") {
      @Override
      public void handlePOST(CoapExchange exchange) {
        serverRequested = true;
        if (returnErrorResponse) {
          exchange.respond(CoAP.ResponseCode.INTERNAL_SERVER_ERROR);
          return;
        }
        requestPayload = new String(exchange.getRequestPayload());
        exchange.respond(CoAP.ResponseCode.VALID);
      }
    });
    resourceURl = "coap://localhost:" + port + "/test";
    coapServer.start();
  }

  @After
  public void tearDown() throws Exception {
    coapServer.stop();
  }

  public CoapClientTargetConfig getConf(String url) {
    CoapClientTargetConfig conf = new CoapClientTargetConfig();
    conf.resourceUrl = url;
    conf.coapMethod = HttpMethod.POST;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    conf.dataGeneratorFormatConfig.textFieldPath = "/";

    return conf;
  }


  @Test
  public void testWithValidCoapUrl() throws Exception {
    CoapClientTargetConfig config = getConf(resourceURl);
    serverRequested = false;
    requestPayload = null;
    returnErrorResponse = false;
    testCoapTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("b"));
  }

  private void testCoapTarget(CoapClientTargetConfig config) throws Exception {
    CoapClientTarget target = new CoapClientTarget(config);
    TargetRunner runner = new TargetRunner.Builder(CoapClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> input = new ArrayList<>();
    input.add(TestHttpClientTarget.createRecord("a"));
    input.add(TestHttpClientTarget.createRecord("b"));
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidURL() throws Exception {
    CoapClientTargetConfig config = getConf("coap://localhost:3434/fdfd");
    CoapClientTarget target = new CoapClientTarget(config);
    TargetRunner runner = new TargetRunner.Builder(CoapClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertFalse(issues.isEmpty());
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testServerError() throws Exception {
    CoapClientTargetConfig config = getConf(resourceURl);
    returnErrorResponse = true;
    testErrorHandling(config);
  }

  private void testErrorHandling(CoapClientTargetConfig config) throws Exception {
    CoapClientTarget target = new CoapClientTarget(config);
    TargetRunner runner = new TargetRunner.Builder(CoapClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> input = new ArrayList<>();
    input.add(TestHttpClientTarget.createRecord("a"));
    input.add(TestHttpClientTarget.createRecord("b"));
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertFalse(runner.getErrorRecords().isEmpty());
      List<Record> records = runner.getErrorRecords();
      Assert.assertEquals(records.size(), 2);
    } finally {
      runner.runDestroy();
    }
  }
}
