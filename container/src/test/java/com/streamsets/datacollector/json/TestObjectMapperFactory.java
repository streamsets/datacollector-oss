/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.json;


import com.codahale.metrics.MetricRegistry;
import org.junit.Assert;
import org.junit.Test;

public class TestObjectMapperFactory {

  @Test
  public void testMetricsSerializers() throws Exception {
    MetricRegistry m = new MetricRegistry();
    m.timer("a");
    m.meter("b");
    m.histogram("c");
    m.counter("d");
    Assert.assertEquals(1, m.getCounters().size());
    Assert.assertEquals(1, m.getMeters().size());
    Assert.assertEquals(1, m.getHistograms().size());
    Assert.assertEquals(1, m.getCounters().size());

    Assert.assertEquals(
        MetricsObjectMapperFactory.getOneLine().writeValueAsString(m),
        ObjectMapperFactory.getOneLine().writeValueAsString(m)
    );

  }

}
