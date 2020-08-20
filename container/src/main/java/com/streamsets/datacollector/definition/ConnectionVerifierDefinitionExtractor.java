/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.ConnectionVerifierDef;

public abstract class ConnectionVerifierDefinitionExtractor {

  private static final ConnectionVerifierDefinitionExtractor EXTRACTOR = new ConnectionVerifierDefinitionExtractor() { };

  public static ConnectionVerifierDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public ConnectionVerifierDefinition extract(StageLibraryDefinition libraryDef, Class<?> klass) {
    ConnectionVerifierDef verifierDef = klass.getAnnotation(ConnectionVerifierDef.class);
    return new ConnectionVerifierDefinition(
        klass.getCanonicalName(),
        verifierDef.connectionFieldName(),
        verifierDef.connectionSelectionFieldName(),
        verifierDef.verifierType(),
        libraryDef.getName()
    );
  }
}