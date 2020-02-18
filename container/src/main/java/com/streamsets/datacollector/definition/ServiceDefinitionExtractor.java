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
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts Service definition.
 */
public class ServiceDefinitionExtractor {

  private static final ServiceDefinitionExtractor EXTRACTOR = new ServiceDefinitionExtractor() {};

  public static ServiceDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public ServiceDefinition extract(
    StageLibraryDefinition libraryDef,
    Class<? extends Service> klass
  ) {
    List<ErrorMessage> errors = validate(libraryDef, klass);
    if(!errors.isEmpty()) {
      throw new IllegalArgumentException(Utils.format("Invalid service definition: {}", errors));
    }

    ServiceDef def = klass.getAnnotation(ServiceDef.class);
    Class provides = def.provides();
    String name = getStageName(klass);
    String label = def.label();
    String description = def.description();
    int version = def.version();
    boolean privateClassloader = def.privateClassLoader();
    ConfigGroupDefinition configGroupDefinition = ConfigGroupExtractor.get().extract(klass, "Service Definition");
    List<ConfigDefinition> configDefinitions = ConfigDefinitionExtractor.get().extract(klass, StageDefinitionExtractor.getGroups(klass),  "Service Definition");

    StageUpgrader upgrader;
    try {
      if (def.upgraderDef().isEmpty()) {
        upgrader = def.upgrader().newInstance();
      } else {
        upgrader = new SelectorStageUpgrader(
            name,
            def.upgrader().newInstance(),
            klass.getClassLoader().getResource(def.upgraderDef())
        );
      }
    } catch (Exception ex) {
      throw new IllegalArgumentException(Utils.format(
          "Could not instantiate StageUpgrader for ServiceDefinition '{}': {}", name, ex.toString()), ex);
    }


    return new ServiceDefinition(
      libraryDef,
      klass,
      provides,
      libraryDef.getClassLoader(),
      version,
      label,
      description,
      configGroupDefinition,
      configDefinitions,
      privateClassloader,
      upgrader
    );
  }

  private List<ErrorMessage> validate(StageLibraryDefinition libraryDef, Class<? extends Service> klass) {
    List<ErrorMessage> errors = new ArrayList<>();

    ServiceDef def = klass.getAnnotation(ServiceDef.class);

    if(def == null) {
      errors.add(new ErrorMessage(DefinitionError.DEF_300, klass.getCanonicalName()));
      return errors;
    }

    if(!def.provides().isAssignableFrom(klass)) {
      errors.add(new ErrorMessage(DefinitionError.DEF_500, klass.getCanonicalName(), def.provides().getCanonicalName()));
    }

    return errors;
  }

  static String getStageName(Class klass) {
    return klass.getName().replace(".", "_").replace("$", "_");
  }
}
