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

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;

@Named("groovy-sdc-indy")
@Singleton
public class StreamSetsGroovyIndyScriptEngineFactory extends StreamSetsGroovyScriptEngineFactory {
  private static final List<String> NAMES = ImmutableList.of("groovy-sdc-indy");

  @Override
  public boolean useInvokeDynamic() {
    return true;
  }

  @Override
  public List<String> getNames() {
    return NAMES;
  }
}
