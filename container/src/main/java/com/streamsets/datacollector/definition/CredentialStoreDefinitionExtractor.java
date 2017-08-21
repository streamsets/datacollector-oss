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

import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;

/**
 * Extracts CredentialStore definition.
 */
public abstract class CredentialStoreDefinitionExtractor {

  private static final CredentialStoreDefinitionExtractor EXTRACTOR = new CredentialStoreDefinitionExtractor() {};

  public static CredentialStoreDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public CredentialStoreDefinition extract(
    StageLibraryDefinition libraryDef,
    Class<? extends CredentialStore> klass
  ) {
    CredentialStoreDef def = klass.getAnnotation(CredentialStoreDef.class);
    String name = StageDefinitionExtractor.getStageName(klass);
    String label = def.label();
    String description = def.description();

    return new CredentialStoreDefinition(libraryDef, klass, name, label, description);
  }

}
