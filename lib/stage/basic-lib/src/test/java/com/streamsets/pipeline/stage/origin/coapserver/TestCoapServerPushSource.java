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
package com.streamsets.pipeline.stage.origin.coapserver;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.testing.NetworkUtils;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestCoapServerPushSource {

  @Test
  public void testSource() throws Exception {
    CoapServerConfigs coapServerConfigs = new CoapServerConfigs();
    coapServerConfigs.resourceName = () -> "sdc";
    coapServerConfigs.port = NetworkUtils.getRandomPort();
    coapServerConfigs.maxConcurrentRequests = 1;
    CoapServerPushSource source =
        new CoapServerPushSource(coapServerConfigs, DataFormat.TEXT, new DataParserFormatConfig());
    final PushSourceRunner runner =
        new PushSourceRunner.Builder(CoapServerDPushSource.class, source).addOutputLane("a").build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });

      URI coapURI = new URI("coap://localhost:" + coapServerConfigs.port + "/" + coapServerConfigs.resourceName.get());
      CoapClient client = new CoapClient(coapURI);
      CoapResponse response = client.post("Hello", MediaTypeRegistry.TEXT_PLAIN);
      Assert.assertNotNull(response);
      Assert.assertEquals(response.getCode(), CoAP.ResponseCode.VALID);

      runner.waitOnProduce();
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("Hello", records.get(0).get("/text").getValue());

    } finally {
      runner.runDestroy();
    }
  }


}
