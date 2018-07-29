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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

/**
 * This enum represents the type of pagination supported by the requested HTTP resource.
 *
 * NONE - Disabled
 * LINK_HEADER - Uses the Link response header to determine the URL of the next page
 *
 * (Future) BY_PAGE - Manual query parameter is used to track page offsets
 * (Future) BY_OFFSET - Manual query parameter is used to track individual result offsets
 */
@GenerateResourceBundle
public enum PaginationMode implements Label {
  NONE("None"),
  LINK_HEADER("Link HTTP Header"),
  LINK_FIELD("Link in Response Field"),
  BY_PAGE("By Page Number"),
  BY_OFFSET("By Offset Number")
  ;

  private final String label;

  PaginationMode(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
