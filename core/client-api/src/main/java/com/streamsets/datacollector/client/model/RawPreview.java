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

import com.streamsets.datacollector.client.StringUtil;



import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
public class RawPreview   {

  private String mimeType = null;
  private String previewData = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("mimeType")
  public String getMimeType() {
    return mimeType;
  }
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("previewData")
  public String getPreviewData() {
    return previewData;
  }
  public void setPreviewData(String previewData) {
    this.previewData = previewData;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class RawPreview {\n");

    sb.append("    mimeType: ").append(StringUtil.toIndentedString(mimeType)).append("\n");
    sb.append("    previewData: ").append(StringUtil.toIndentedString(previewData)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
