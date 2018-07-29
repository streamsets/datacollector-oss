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
package com.streamsets.pipeline.stage.processor.aggregation.aggregator;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAggregator {

  private static class MyAggregator extends Aggregator<MyAggregator, Long> {

    public MyAggregator(String name, GroupByAggregator groupByParent) {
      super(Long.class, name);
    }

    @Override
    public AggregatorData createAggregatorData(long timeWindowMillis) {
      return null;
    }
  }

  @Test
  public void testMethods() {
    GroupByAggregator parent = Mockito.mock(GroupByAggregator.class);
    AggregatorDataProvider dataProvider = Mockito.mock(AggregatorDataProvider.class);

    Aggregator aggregator = new MyAggregator("name", parent);
    aggregator.setDataProvider(dataProvider);

    Assert.assertEquals(Long.class, aggregator.getValueType());
    Assert.assertEquals("name", aggregator.getName());
    Assert.assertEquals(dataProvider, aggregator.getDataProvider());

    AggregatorData<MyAggregator, Long> aggregatorData = Mockito.mock(AggregatorData.class);
    Mockito.when(aggregatorData.get()).thenReturn(1L);
    Aggregator.Aggregatable<MyAggregator> aggregatable = Mockito.mock(Aggregator.Aggregatable.class);
    Mockito.when(aggregatorData.getAggregatable()).thenReturn(aggregatable);
    Mockito.when(dataProvider.getData(Mockito.eq(aggregator))).thenReturn(aggregatorData);

    Assert.assertEquals(dataProvider, aggregator.getDataProvider());

    Assert.assertEquals(aggregatorData, aggregator.getData());

    Assert.assertEquals(1L, aggregator.get());

    Assert.assertEquals(aggregatable, aggregator.getAggregatable());

    Aggregator.Aggregatable<MyAggregator> aggregatable1 = Mockito.mock(Aggregator.Aggregatable.class);

    aggregator.aggregate(aggregatable1);
    Mockito.verify(aggregatorData, Mockito.times(1)).aggregate(Mockito.eq(aggregatable1));

    Assert.assertNotNull(aggregator.toString());
  }

}
