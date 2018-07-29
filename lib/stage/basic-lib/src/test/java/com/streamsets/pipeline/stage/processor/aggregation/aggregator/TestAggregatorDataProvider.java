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

import com.streamsets.pipeline.stage.processor.aggregation.WindowType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class TestAggregatorDataProvider {

  @Test
  public void testDataWindow() {
//    CountAggregator aggregator = new CountAggregator("count", null);
//    AggregatorDataProvider provider = new AggregatorDataProvider(1);
//    aggregator.setDataProvider(provider);
//    provider.addAggregator(aggregator);
//    provider.start(1L);
//    aggregator.process(1L);
//
//    AggregatorDataProvider.DataWindow dataWindow = provider.createDataWindow(2);
//
//    Assert.assertFalse(dataWindow.isClosed());
//
//    Assert.assertNotNull(dataWindow.getAllGroupByElementAggregators());
//    Assert.assertEquals(2L, dataWindow.getEndTimeMillis());
//
//    GroupByAggregator parent = Mockito.mock(GroupByAggregator.class);
//    Assert.assertTrue(dataWindow.getGroupByElementAggregators(parent).isEmpty());
//
//    Assert.assertEquals(aggregator.getData(), dataWindow.getData(aggregator));
//
//    // group-by logic
//
//    GroupByAggregator groupByAggregator = Mockito.mock(GroupByAggregator.class);
//    Map<String, Aggregator> groupByElements = new HashMap<>();
//    Aggregator groupByElementAggregator = Mockito.mock(Aggregator.class);
//    groupByElements.put("a", groupByElementAggregator);
//    dataWindow.getAllGroupByElementAggregators().put(groupByAggregator, groupByElements);
//
//    Assert.assertNotSame(groupByElements, dataWindow.getGroupByElementAggregators(groupByAggregator));
//    Assert.assertEquals(groupByElements, dataWindow.getGroupByElementAggregators(groupByAggregator));
//
//    // closing the datawindow
//
//    AggregatorData aggregatorData = aggregator.createAggregatorData(1L);
//    Map<Aggregator, AggregatorData> map = new HashMap<>();
//    map.put(aggregator, aggregatorData);
//
//    dataWindow.setDataAndClose(map);
//
//    Assert.assertTrue(dataWindow.isClosed());
//
//    Assert.assertEquals(aggregatorData, dataWindow.getData(aggregator));
//
//    Assert.assertSame(groupByElements, dataWindow.getGroupByElementAggregators(groupByAggregator));
  }

  @Test
  public void testProviderAddAggregatorOk() {
    Aggregator aggregator = Mockito.mock(Aggregator.class);
    Mockito.when(aggregator.getName()).thenReturn("aggregator");
    Mockito.when(aggregator.createAggregatorData(Mockito.eq(1L))).thenReturn(Mockito.mock(AggregatorData.class));
    GroupByAggregator groupByAggregator = Mockito.mock(GroupByAggregator.class);
    Mockito.when(groupByAggregator.getName()).thenReturn("groupByAggregator");
    Mockito.when(groupByAggregator.createAggregatorData(Mockito.eq(1L))).thenReturn(Mockito.mock(AggregatorData.class));
    AggregatorDataProvider provider = new AggregatorDataProvider(1, WindowType.ROLLING);

    provider.addAggregator(aggregator);
    provider.addAggregator(groupByAggregator);
    provider.start(1L);
    provider.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testProviderAddAggregatorFail() {
    Aggregator aggregator = Mockito.mock(Aggregator.class);
    AggregatorDataProvider provider = new AggregatorDataProvider(1, WindowType.ROLLING);

    provider.start(1L);
    provider.addAggregator(aggregator);
  }

  @Test
  public void testStart() {
    CountAggregator aggregator = new CountAggregator("count");
    AggregatorDataProvider provider = new AggregatorDataProvider(2, WindowType.ROLLING);
    provider = Mockito.spy(provider);

    aggregator.setDataProvider(provider);
    provider.addAggregator(aggregator);

    provider.start(1L);
    Mockito.verify(provider, Mockito.times(1)).roll(Mockito.eq(1L));

    Map<Aggregator, AggregatorData> providerData = provider.get();
    List<AggregatorDataProvider.DataWindow> dataWindows = provider.getDataWindows();
    AggregatorData aggregatorData = provider.getData(aggregator);

    Assert.assertNotNull(providerData);
    Assert.assertNotNull(dataWindows);
    Assert.assertNotNull(aggregatorData);

    Assert.assertEquals(1, providerData.size());
    Assert.assertEquals(aggregatorData, providerData.get(aggregator));
    Assert.assertEquals(1, dataWindows.size());
    Assert.assertFalse(dataWindows.get(0).isClosed());

    provider.stop();
  }

  @Test
  public void testRoll() {
    CountAggregator aggregator = new CountAggregator("count");
    AggregatorDataProvider provider = new AggregatorDataProvider(2, WindowType.ROLLING);
    provider = Mockito.spy(provider);

    aggregator.setDataProvider(provider);
    provider.addAggregator(aggregator);

    provider.start(1L);
    Mockito.verify(provider, Mockito.times(1)).roll(Mockito.eq(1L));

    Map<Aggregator, AggregatorData> providerData = provider.get();
    List<AggregatorDataProvider.DataWindow> dataWindows = provider.getDataWindows();
    AggregatorData aggregatorData = provider.getData(aggregator);

    provider.roll(2L);

    Map<Aggregator, AggregatorData> newProviderData = provider.get();
    List<AggregatorDataProvider.DataWindow> newDataWindows = provider.getDataWindows();
    AggregatorData newAggregatorData = provider.getData(aggregator);

    Assert.assertNotNull(newProviderData);
    Assert.assertNotNull(newDataWindows);
    Assert.assertNotNull(newAggregatorData);

    Assert.assertNotSame(providerData, newProviderData);
    Assert.assertNotSame(dataWindows, newDataWindows);
    Assert.assertNotSame(aggregatorData, newAggregatorData);

    Assert.assertEquals(1, newProviderData.size());
    Assert.assertEquals(newAggregatorData, newProviderData.get(aggregator));
    Assert.assertEquals(2, newDataWindows.size());
    Assert.assertEquals(dataWindows.get(0), newDataWindows.get(0));
    Assert.assertTrue(newDataWindows.get(0).isClosed());
    Assert.assertFalse(newDataWindows.get(1).isClosed());
    Assert.assertTrue(dataWindows.get(0).isClosed());

    provider.stop();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetDataTopAggregatorFail() {
    AggregatorDataProvider provider = new AggregatorDataProvider(1, WindowType.ROLLING);
    provider.start(1L);

    CountAggregator aggregator = new CountAggregator("count");
    provider.getData(aggregator);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetDataGroupByElementAggregatorFail() {
    AggregatorDataProvider provider = new AggregatorDataProvider(1, WindowType.ROLLING);
    provider.start(1L);

    CountAggregator aggregator = new CountAggregator("count");
    provider.getData(aggregator);
  }

  @Test
  public void testGetDataOk() {
    Aggregator aggregator = Mockito.mock(Aggregator.class);
    Mockito.when(aggregator.getName()).thenReturn("aggregator");
    Mockito.when(aggregator.createAggregatorData(Mockito.eq(1L))).thenReturn(Mockito.mock(AggregatorData.class));
    GroupByAggregator groupByAggregator = Mockito.mock(GroupByAggregator.class);
    Mockito.when(groupByAggregator.getName()).thenReturn("groupByAggregator");
    Mockito.when(groupByAggregator.createAggregatorData(Mockito.eq(1L))).thenReturn(Mockito.mock(AggregatorData.class));
    AggregatorDataProvider provider = new AggregatorDataProvider(1, WindowType.ROLLING);

    provider.addAggregator(aggregator);
    provider.addAggregator(groupByAggregator);
    provider.start(1L);

    Assert.assertNotNull(provider.getData(aggregator));

    Aggregator groupByElementAggregator = Mockito.mock(Aggregator.class);
    Mockito.when(aggregator.getName()).thenReturn("groupByAggregatorElement");
    AggregatorData aggregatorData = Mockito.mock(AggregatorData.class);
    Mockito.when(groupByElementAggregator.createAggregatorData(Mockito.eq(1L))).thenReturn(aggregatorData);

    Assert.assertNotNull(provider.getData(aggregator));
  }

}
