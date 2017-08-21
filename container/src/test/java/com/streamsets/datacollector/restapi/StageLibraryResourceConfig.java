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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public class StageLibraryResourceConfig extends AbstractBinder {
  @Override
  protected void configure() {
    bindFactory(TestUtil.StageLibraryTestInjector.class).to(StageLibraryTask.class);
    bindFactory(TestUtil.RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
    bindFactory(TestUtil.BuildInfoTestInjector.class).to(BuildInfo.class);
  }
}
