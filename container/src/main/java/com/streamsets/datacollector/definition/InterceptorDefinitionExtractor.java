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

import com.streamsets.datacollector.config.InterceptorDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import com.streamsets.pipeline.api.interceptor.InterceptorDef;

import java.util.ArrayList;
import java.util.List;

public class InterceptorDefinitionExtractor {

  private static final InterceptorDefinitionExtractor EXTRACTOR = new InterceptorDefinitionExtractor() {};

  public static InterceptorDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public InterceptorDefinition extract(
    StageLibraryDefinition libraryDef,
    Class<? extends Interceptor> klass
  ) {
    List<ErrorMessage> errors = validate(libraryDef, klass);
    if(!errors.isEmpty()) {
      throw new IllegalArgumentException(Utils.format("Invalid Interceptor definition: {}", errors));
    }

    InterceptorDef def = klass.getAnnotation(InterceptorDef.class);
    int version = def.version();
    Class<? extends InterceptorCreator> defaultCreator = def.creator();

    return new InterceptorDefinition(
      libraryDef,
      klass,
      libraryDef.getClassLoader(),
      version,
      defaultCreator
    );
  }

  private List<ErrorMessage> validate(
    StageLibraryDefinition libraryDef,
    Class<? extends Interceptor> klass
  ) {
    List<ErrorMessage> errors = new ArrayList<>();

    InterceptorDef def = klass.getAnnotation(InterceptorDef.class);

    if(def == null) {
      errors.add(new ErrorMessage(DefinitionError.DEF_300, klass.getCanonicalName()));
      return errors;
    }

    return errors;
  }

}
