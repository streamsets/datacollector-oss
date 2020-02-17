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
package com.streamsets.datacollector.restapi.rbean.lang;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

public abstract class RBean<B extends RBean> extends RType<B> {
  private Long dataVersion;
  private String readOnlySignature;
  private boolean failed;

  /**
   * RBean subclasses, different than EBean/OLEBean subclasses, must implement this method in order to define
   * the @UiProperty annotated 'id' instance property.
   * @return
   */
  public abstract RString getId();

  /**
   * Get acl Id for a resource
   */
  @JsonIgnore
  public RString getAclId() {
    return getId();
  }

  public void setAclId(RString id) {
    // NOP
  }

  /**
   * We don't use the fluent pattern for this setter because SELMA generator gets confused
   * @param id
   */
  public abstract void setId(RString id);

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getDataVersion() {
    return dataVersion;
  }

  /**
   * We don't use the fluent pattern for this setter because SELMA generator gets confused
   */
  public void setDataVersion(Long dataVersion) {
    this.dataVersion = dataVersion;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getReadOnlySignature() {
    return readOnlySignature;
  }

  public void setReadOnlySignature(String readOnlySignature) {
    this.readOnlySignature = readOnlySignature;
  }

  /*
   * Not using getter syntax to stop Jackson and Selma for processing it as a property
   */
  @JsonIgnore
  public boolean hasFailed() {
    return failed;
  }

  /**
   * To indicate the backend operation failed on the bean and trigger an HTTP 499
   * (RestResponseFilter.X_HTTP_DATA_VALIDATION_ERROR)
   */
  public void fail() {
    failed = true;
  }

}
