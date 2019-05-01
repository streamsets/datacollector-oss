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
package com.streamsets.datacollector.antennadoctor.engine.el;

import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.util.Version;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

public class VersionEL {
  private static final String CONTEXT_VAR = "__content_version_buildInfo";

  public static void setVars(ELVars variables, BuildInfo buildInfo) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(CONTEXT_VAR, buildInfo);
  }

  public static BuildInfo getBuildInfo() {
    return (BuildInfo) ELEval.getVariablesInScope().getContextVariable(CONTEXT_VAR);
  }

  @ElFunction(prefix = "version", name = "isGreaterOrEqualTo")
  public static boolean isGreaterOrEqualTo(@ElParam("version") String version) {
    return new Version(getBuildInfo().getVersion()).isGreaterOrEqualTo(version);
  }

  @ElFunction(prefix = "version", name = "isGreaterThan")
  public static boolean isGreateThan(@ElParam("version") String version) {
    return new Version(getBuildInfo().getVersion()).isGreaterThan(version);
  }

  @ElFunction(prefix = "version", name = "isLessOrEqualTo")
  public static boolean isLessOrEqualTo(@ElParam("version") String version) {
    return new Version(getBuildInfo().getVersion()).isLessOrEqualTo(version);
  }

  @ElFunction(prefix = "version", name = "isLessThan")
  public static boolean isLessThan(@ElParam("version") String version) {
    return new Version(getBuildInfo().getVersion()).isLessThan(version);
  }
}
