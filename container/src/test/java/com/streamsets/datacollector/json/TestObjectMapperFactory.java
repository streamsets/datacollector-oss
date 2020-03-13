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
import com.streamsets.datacollector.metrics.ExtendedMeter;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class TestObjectMapperFactory {

  @Test
  public void testMetricsSerializersNotOverriden() throws Exception {
    MetricRegistry metricsRegistry = new MetricRegistry();
    metricsRegistry.timer("a");
    metricsRegistry.meter("b");
    metricsRegistry.histogram("c");
    metricsRegistry.counter("d");
    Assert.assertEquals(1, metricsRegistry.getCounters().size());
    Assert.assertEquals(1, metricsRegistry.getMeters().size());
    Assert.assertEquals(1, metricsRegistry.getHistograms().size());
    Assert.assertEquals(1, metricsRegistry.getCounters().size());

    Assert.assertEquals(
        MetricsObjectMapperFactory.getOneLine().writeValueAsString(metricsRegistry),
        ObjectMapperFactory.getOneLine().writeValueAsString(metricsRegistry)
    );

    Assert.assertArrayEquals(
        MetricsObjectMapperFactory.getOneLine().writeValueAsBytes(metricsRegistry),
        ObjectMapperFactory.getOneLine().writeValueAsBytes(metricsRegistry)
    );

    BigDecimal b = new BigDecimal("5.20");
    Assert.assertArrayEquals(
        MetricsObjectMapperFactory.getOneLine().writeValueAsBytes(b),
        ObjectMapperFactory.get().writeValueAsBytes(b)
    );

    ExtendedMeter extendedMeter = new ExtendedMeter();
    Assert.assertEquals(
        MetricsObjectMapperFactory.getOneLine().writeValueAsString(extendedMeter),
        ObjectMapperFactory.getOneLine().writeValueAsString(extendedMeter)
    );

    Assert.assertArrayEquals(
        MetricsObjectMapperFactory.getOneLine().writeValueAsBytes(extendedMeter),
        ObjectMapperFactory.getOneLine().writeValueAsBytes(extendedMeter)
    );

  }

}
