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

package com.streamsets.pipeline.lib.parser.net.netflow.v9;

import java.util.ArrayList;
import java.util.List;

public class FlowSetTemplate {

  private final FlowKind templateKind;
  private final int templateId;
  private final List<NetflowV9FieldTemplate> fieldTemplates = new ArrayList<>();
  private final int totalFieldsLength;

  public FlowSetTemplate(FlowKind templateKind, int templateId, List<NetflowV9FieldTemplate> fieldTemplates) {
    this.templateKind = templateKind;
    this.templateId = templateId;
    int totalLength = 0;
    if (fieldTemplates != null) {
      this.fieldTemplates.addAll(fieldTemplates);
      for (NetflowV9FieldTemplate template : fieldTemplates) {
        totalLength += template.getLength();
      }
    }
    totalFieldsLength = totalLength;
  }

  public int getTemplateId() {
    return templateId;
  }

  public List<NetflowV9FieldTemplate> getFieldTemplates() {
    return fieldTemplates;
  }

  public int getTotalFieldsLength() {
    return totalFieldsLength;
  }
}
