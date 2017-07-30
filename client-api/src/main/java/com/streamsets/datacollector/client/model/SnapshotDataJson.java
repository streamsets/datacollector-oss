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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;


@ApiModel(description = "")
public class SnapshotDataJson   {

  private List<List<StageOutputJson>> snapshotBatches = new ArrayList<List<StageOutputJson>>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("snapshotBatches")
  public List<List<StageOutputJson>> getSnapshotBatches() {
    return snapshotBatches;
  }
  public void setSnapshotBatches(List<List<StageOutputJson>> snapshotBatches) {
    this.snapshotBatches = snapshotBatches;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class SnapshotDataJson {\n");

    sb.append("    snapshotBatches: ").append(StringUtil.toIndentedString(snapshotBatches)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
