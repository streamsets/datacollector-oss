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
package com.streamsets.datacollector.util;

import java.io.File;
import java.util.List;

import com.streamsets.pipeline.util.SystemProcess;

public class SystemProcessFactory {
  public SystemProcess create(String name, File tempDir, List<String> args) {
    SystemProcess systemProcess;
    if (Boolean.getBoolean("sdc.testing-mode")) {
      systemProcess = new MiniSDCSystemProcessImpl(name, tempDir, args, new File(System.getProperty("test.data.dir")));
    } else {
      systemProcess = new SystemProcessImpl(name, tempDir, args);
    }
    return systemProcess;
  }
}
