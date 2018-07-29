/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.usagestats;

import com.streamsets.datacollector.bundles.BundleWriter;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestStatsGenerator {

  @Test
  public void testGenerator() throws IOException {
    BundleWriter writer = Mockito.mock(BundleWriter.class);

    List<StatsBean> stats = new ArrayList<>();

    StatsGenerator generator = new StatsGenerator(stats);

    generator.generateContent(null, writer);

    Mockito.verify(writer, Mockito.times(1)).writeJson(Mockito.eq("sdc-stats.json"), Mockito.eq(stats));
  }

}
