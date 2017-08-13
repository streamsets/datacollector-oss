/**
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
package com.streamsets.pipeline.stage.processor.schemagen.generators;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.processor.schemagen.config.SchemaGeneratorConfig;

/**
 * Abstract class for generating schema where each schema type will be a subclass.
 */
public abstract class SchemaGenerator {

  /**
   * Configuration of the generator.
   */
  private SchemaGeneratorConfig config;

  public void setConfig(SchemaGeneratorConfig config) {
    this.config = config;
  }

  public SchemaGeneratorConfig getConfig() {
    return this.config;
  }

  /**
   * Generate schema for given record.
   *
   * @param record Input record for which a schema should bbe generated.
   * @return String representation of the schema
   * @throws OnRecordErrorException If schema can't bbe generated for any reason
   */
  public abstract String generateSchema(Record record) throws OnRecordErrorException;
}
