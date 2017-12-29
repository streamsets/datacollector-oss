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

import com.streamsets.pipeline.stage.processor.aggregation.AggregationFunction;
import org.junit.Assert;
import org.junit.Test;

public class TestAggregationFunction {

  @Test
  public void testCount() {
    Assert.assertEquals("COUNT", AggregationFunction.COUNT.getLabel());
    Assert.assertEquals(CountAggregator.class, AggregationFunction.COUNT.getAggregatorClass());
  }

  @Test
  public void testAvgDouble() {
    Assert.assertEquals("AVG (double)", AggregationFunction.AVG_DOUBLE.getLabel());
    Assert.assertEquals(DoubleAvgAggregator.class, AggregationFunction.AVG_DOUBLE.getAggregatorClass());
  }

  @Test
  public void testAvgInteger() {
    Assert.assertEquals("AVG (int)", AggregationFunction.AVG_INTEGER.getLabel());
    Assert.assertEquals(LongAvgAggregator.class, AggregationFunction.AVG_INTEGER.getAggregatorClass());
  }

  @Test
  public void testStdDev() {
    Assert.assertEquals("StdDev", AggregationFunction.STD_DEV.getLabel());
    Assert.assertEquals(DoubleStdDevAggregator.class, AggregationFunction.STD_DEV.getAggregatorClass());
  }

  @Test
  public void testMinDouble() {
    Assert.assertEquals("MIN (double)", AggregationFunction.MIN_DOUBLE.getLabel());
    Assert.assertEquals(DoubleMinAggregator.class, AggregationFunction.MIN_DOUBLE.getAggregatorClass());
  }

  @Test
  public void testMinInteger() {
    Assert.assertEquals("MIN (int)", AggregationFunction.MIN_INTEGER.getLabel());
    Assert.assertEquals(LongMinAggregator.class, AggregationFunction.MIN_INTEGER.getAggregatorClass());
  }

  @Test
  public void testMaxDouble() {
    Assert.assertEquals("MAX (double)", AggregationFunction.MAX_DOUBLE.getLabel());
    Assert.assertEquals(DoubleMaxAggregator.class, AggregationFunction.MAX_DOUBLE.getAggregatorClass());
  }

  @Test
  public void testMaxInteger() {
    Assert.assertEquals("MAX (int)", AggregationFunction.MAX_INTEGER.getLabel());
    Assert.assertEquals(LongMaxAggregator.class, AggregationFunction.MAX_INTEGER.getAggregatorClass());
  }

  @Test
  public void testSumDouble() {
    Assert.assertEquals("SUM (double)", AggregationFunction.SUM_DOUBLE.getLabel());
    Assert.assertEquals(DoubleSumAggregator.class, AggregationFunction.SUM_DOUBLE.getAggregatorClass());
  }

  @Test
  public void testSumInteger() {
    Assert.assertEquals("SUM (int)", AggregationFunction.SUM_INTEGER.getLabel());
    Assert.assertEquals(LongSumAggregator.class, AggregationFunction.SUM_INTEGER.getAggregatorClass());
  }
}
