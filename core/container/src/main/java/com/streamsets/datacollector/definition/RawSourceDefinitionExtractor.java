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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class RawSourceDefinitionExtractor {

  private static final RawSourceDefinitionExtractor EXTRACTOR = new RawSourceDefinitionExtractor() {};

  public static RawSourceDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ErrorMessage> validate(Class<? extends Stage> klass, Object contextMsg) {
    return new ArrayList<>();
  }

  public RawSourceDefinition extract(Class<? extends Stage> klass, Object contextMsg) {
    List<ErrorMessage> errors = validate(klass, contextMsg);
    if (errors.isEmpty()) {
      RawSourceDefinition rDef = null;
      RawSource rawSource = klass.getAnnotation(RawSource.class);
      if (rawSource != null) {
        Class rsKlass = rawSource.rawSourcePreviewer();
        List<ConfigDefinition> configDefs = ConfigDefinitionExtractor.get().
            extract(rsKlass, Collections.<String>emptyList(), Utils.formatL("{} RawSource='{}'", contextMsg,
                                                                            rsKlass.getSimpleName()));
        rDef = new RawSourceDefinition(rawSource.rawSourcePreviewer().getName(), rawSource.mimeType(), configDefs);
      }
      return rDef;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid RawSourceDefinition: {}", errors));
    }
  }

}
