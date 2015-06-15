/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.RawSourceDefinition;

import java.util.ArrayList;
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
            extract(rsKlass, Utils.formatL("{} RawSource='{}'", contextMsg, rsKlass.getSimpleName()));
        rDef = new RawSourceDefinition(rawSource.rawSourcePreviewer().getName(), rawSource.mimeType(), configDefs);
      }
      return rDef;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid RawSourceDefinition: {}", errors));
    }
  }

}
