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

import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;

import java.io.IOException;

@BundleContentGeneratorDef(
    name = "Stats",
    description = "Stats Generator",
    version = 1,
    enabledByDefault = false
)
public class StatsGenerator implements BundleContentGenerator {
  public StatsGenerator() {
  }

  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    writer.writeJson("sdc-stats.json", context.getStatsCollector().getStatsInfo().snapshot().getCollectedStats());
  }

}
