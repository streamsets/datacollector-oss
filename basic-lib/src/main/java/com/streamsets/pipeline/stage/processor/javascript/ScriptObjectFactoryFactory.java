/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import javax.script.ScriptEngine;

public class ScriptObjectFactoryFactory {

  private static double JAVA_VERSION = getVersion();

  private ScriptObjectFactoryFactory() {}

  static double getVersion () {
    String version = System.getProperty("java.version");
    int pos = version.indexOf('.');
    pos = version.indexOf('.', pos+1);
    return Double.parseDouble (version.substring (0, pos));
  }

  public static ScriptObjectFactory getScriptObjectFactory(ScriptEngine engine, Stage.Context context) {
    double version = getVersion();
    if (version >= 1.8) {
      return new Java8JavaScriptObjectFactory(engine, context);
    } else {
      return new Java7JavaScriptObjectFactory(engine, context);
    }
  }
}
