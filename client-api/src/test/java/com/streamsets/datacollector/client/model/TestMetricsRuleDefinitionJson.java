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

package com.streamsets.datacollector.client.model;

import com.streamsets.datacollector.config.MetricElement;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestMetricsRuleDefinitionJson {

  /* Make sure that this enum class has the same values as com.streamsets.datacollector.config.MetricsRuleDefinition.
   * Prevent one class being updated without updating the other */

  @Test
  public void testMetricsRuleDefinitionJsonValues() {
    List<String> clientApiMetrics =
        Stream.of(MetricsRuleDefinitionJson.MetricElementEnum.values()).map(Enum::name).collect(Collectors.toList());
    List<String> containerMetrics = Stream.of(MetricElement.values()).map(Enum::name).collect(Collectors.toList());

    for (String metric : clientApiMetrics) {
      Assert.assertTrue(containerMetrics.contains(metric));
    }

    for (String metric : containerMetrics) {
      Assert.assertTrue(clientApiMetrics.contains(metric));
    }
  }
}
