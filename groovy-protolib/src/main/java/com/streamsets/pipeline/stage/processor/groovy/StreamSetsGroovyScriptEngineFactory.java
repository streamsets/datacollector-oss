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
package com.streamsets.pipeline.stage.processor.groovy;

import com.google.common.collect.ImmutableList;
import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import javax.inject.Named;
import javax.inject.Singleton;
import javax.script.ScriptEngine;
import java.util.List;
import java.util.Map;

import static org.codehaus.groovy.control.CompilerConfiguration.INVOKEDYNAMIC;
import static org.codehaus.groovy.control.CompilerConfiguration.JDK8;

@Named("groovy-sdc")
@Singleton
public class StreamSetsGroovyScriptEngineFactory extends org.codehaus.groovy.jsr223.GroovyScriptEngineFactory {
  private static final List<String> NAMES = ImmutableList.of("groovy-sdc");

  public boolean useInvokeDynamic() {
    return false;
  }

  @Override
  public ScriptEngine getScriptEngine() {
    CompilerConfiguration conf = new CompilerConfiguration();
    Map<String, Boolean> optimizationOptions = conf.getOptimizationOptions();
    optimizationOptions.put(INVOKEDYNAMIC, useInvokeDynamic());
    conf.setOptimizationOptions(optimizationOptions);
    conf.setTargetBytecode(JDK8);

    GroovyClassLoader classLoader = new GroovyClassLoader(Thread.currentThread().getContextClassLoader(), conf);
    return new GroovyScriptEngineImpl(classLoader);
  }

  @Override
  public List<String> getNames() {
    return NAMES;
  }
}
