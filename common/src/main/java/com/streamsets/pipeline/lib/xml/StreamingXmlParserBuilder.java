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

package com.streamsets.pipeline.lib.xml;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class StreamingXmlParserBuilder {

  private Reader reader = null;
  private String recordElement = null;
  private Map<String, String> namespaces = null;
  private long initialPosition = 0;

  private boolean useFieldAttributesInsteadOfFields = true;
  private boolean preserveRootElement = false;

  public StreamingXmlParserBuilder withReader(Reader reader) {
    this.reader = reader;
    return this;
  }

  public StreamingXmlParserBuilder withRecordElement(String recordElement) {
    this.recordElement = recordElement;
    return this;
  }

  public StreamingXmlParserBuilder withNamespaces(Map<String, String> namespaces) {
    this.namespaces = namespaces;
    return this;
  }

  public StreamingXmlParserBuilder withInitialPosition(long initialPosition) {
    this.initialPosition = initialPosition;
    return this;
  }

  public StreamingXmlParserBuilder withUseFieldAttributesInsteadOfFields(boolean useFieldAttributesInsteadOfFields) {
    this.useFieldAttributesInsteadOfFields = useFieldAttributesInsteadOfFields;
    return this;
  }

  public StreamingXmlParserBuilder withPreserveRootElement(boolean preserveRootElement) {
    this.preserveRootElement = preserveRootElement;
    return this;
  }

  public StreamingXmlParser build() throws IOException, XMLStreamException {
    return new StreamingXmlParser(
        reader,
        recordElement,
        namespaces,
        initialPosition,
        useFieldAttributesInsteadOfFields,
        preserveRootElement
    );
  }
}
