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

import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.lib.el.Base64EL;
import com.streamsets.pipeline.lib.el.CollectionEL;
import com.streamsets.pipeline.lib.el.FileEL;
import com.streamsets.pipeline.lib.el.MathEL;
import com.streamsets.pipeline.lib.el.StringEL;

public class AntennaDoctorELDefinitionExtractor extends ELDefinitionExtractor {
  static final Class[] DEFAULT_ELS = {
      Base64EL.class,
      FileEL.class,
      MathEL.class,
      StringEL.class,
      CollectionEL.class,
      VarEL.class
  };

  private static final ELDefinitionExtractor EXTRACTOR = new AntennaDoctorELDefinitionExtractor(DEFAULT_ELS);

  private AntennaDoctorELDefinitionExtractor(Class[] defaultElDefs) {
    super(defaultElDefs);
  }

  public static ELDefinitionExtractor get() {
    return EXTRACTOR;
  }
}
