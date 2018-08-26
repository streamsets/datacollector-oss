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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.el.JobEL;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.pipeline.lib.el.Base64EL;
import com.streamsets.pipeline.lib.el.FileEL;
import com.streamsets.pipeline.lib.el.MathEL;
import com.streamsets.pipeline.lib.el.StringEL;

public class ConcreteELDefinitionExtractor extends ELDefinitionExtractor {
  static final Class[] DEFAULT_EL_DEFS = {
      Base64EL.class,
      FileEL.class,
      JvmEL.class,
      MathEL.class,
      RuntimeEL.class,
      StringEL.class,
      PipelineEL.class,
      JobEL.class
  };

  private static final ELDefinitionExtractor EXTRACTOR = new ConcreteELDefinitionExtractor(DEFAULT_EL_DEFS) {};

  public static ELDefinitionExtractor get() {
    return EXTRACTOR;
  }

  private ConcreteELDefinitionExtractor(Class[] defaultElDefs) {
    super(defaultElDefs);
  }
}
