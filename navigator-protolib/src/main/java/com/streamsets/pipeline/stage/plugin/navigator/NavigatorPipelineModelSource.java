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
package com.streamsets.pipeline.stage.plugin.navigator;

import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.streamsets.pipeline.api.lineage.LineageEvent;

import java.util.ArrayList;
import java.util.Collection;

@MClass(model = "sdc_pipeline_model_source")
public class NavigatorPipelineModelSource extends NavigatorPipelineModel {

  // this class exists because Navigator SDK pulls an NPE if it is passed
  // a RelationRole.SOURCE or RelationRole.TARGET which is either not initialized,
  // or is initialized, but is empty.

  @MRelation(role= RelationRole.SOURCE)
  private Collection<NavigatorDataset> inputs = new ArrayList<>();

  public NavigatorPipelineModelSource(String namespace, LineageEvent event, String parentId) {
    fillIn(namespace, event, parentId);
  }

  public void addInput(NavigatorDataset dataset) {
    inputs.add(dataset);
  }

  public Collection<NavigatorDataset> getInputs() {
    return inputs;
  }

}
