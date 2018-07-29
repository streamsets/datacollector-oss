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

public class TestDoubleSumAggregator {

  @Test
  public void testAggregator() {
    Aggregators aggregators = new Aggregators(2, WindowType.ROLLING);
    DoubleSumAggregator aggregator = aggregators.createSimple("a", DoubleSumAggregator.class);
    aggregators.start(1);

    Aggregators aggregatorsA = new Aggregators(2, WindowType.ROLLING);
    DoubleSumAggregator aggregatorA = aggregatorsA.createSimple("a", DoubleSumAggregator.class);
    aggregatorsA.start(1);

    Assert.assertEquals("a", aggregator.getName());
    Assert.assertNotNull(aggregator.createAggregatorData(1L));

    Assert.assertNull(aggregator.get());

    aggregator.process(1d);
    Assert.assertEquals(1d, (double) aggregator.get(), 0.0001);

    aggregator.process(2d);
    Assert.assertEquals((Double) 3d, aggregator.get());

    Assert.assertEquals("a", aggregator.getAggregatable().getName());
    Assert.assertEquals(DoubleSumAggregator.DoubleSumAggregatable.class.getSimpleName(), aggregator.getAggregatable().getType());
    Assert.assertEquals(3d, ((DoubleSumAggregator.DoubleSumAggregatable) aggregator.getAggregatable()).getSum(), 0.0001);

    aggregatorA.process(1d);

    aggregatorA.aggregate(aggregator.getAggregatable());
    Assert.assertEquals((Double) 4d, aggregatorA.get(), 0.000001);

    aggregatorsA.stop();

    aggregators.stop();
  }

}
