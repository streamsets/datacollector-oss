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
package com.streamsets.datacollector.restapi.rbean.secrets;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RDatetime;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.datacollector.restapi.rbean.lang.RLong;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.lang.RText;

public class RSecret extends RBean<RSecret> {
  private RString id = new RString();
  private RString vault = new RString();
  private RString name = new RString();
  private REnum<SecretType> type = new REnum<>();
  @JsonIgnore
  private RString uploadedFile;
  private RString value = new RString();
  private RString org = new RString();
  private RText comment = new RText();
  private RString createdBy = new RString();
  private RDatetime createdOn = new RDatetime();
  private RString lastModifiedBy = new RString();
  private RDatetime lastModifiedOn = new RDatetime();
  private RString dek = new RString();
  private RString kek = new RString();
  private RLong algorithm = new RLong();

  @Override
  public RString getId() {
    return id;
  }

  @Override
  public void setId(RString id) {
    this.id = id;
  }

  public RString getVault() {
    return vault;
  }

  public void setVault(RString vault) {
    this.vault = vault;
  }

  public RString getName() {
    return name;
  }

  public void setName(RString name) {
    this.name = name;
  }

  public RString getValue() {
    return value;
  }

  public void setValue(RString value) {
    this.value = value;
  }

  public RString getOrg() {
    return org;
  }

  public void setOrg(RString org) {
    this.org = org;
  }

  public RText getComment() {
    return comment;
  }

  public void setComment(RText comment) {
    this.comment = comment;
  }

  public RDatetime getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(RDatetime createdOn) {
    this.createdOn = createdOn;
  }

  public RString getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(RString createdBy) {
    this.createdBy = createdBy;
  }

  public RDatetime getLastModifiedOn() {
    return lastModifiedOn;
  }

  public void setLastModifiedOn(RDatetime lastModifiedOn) {
    this.lastModifiedOn = lastModifiedOn;
  }

  public RString getLastModifiedBy() {
    return lastModifiedBy;
  }

  public void setLastModifiedBy(RString lastModifiedBy) {
    this.lastModifiedBy = lastModifiedBy;
  }

  public RString getDek() {
    return dek;
  }

  public void setDek(RString dek) {
    this.dek = dek;
  }

  public RString getKek() {
    return kek;
  }

  public void setKek(RString kek) {
    this.kek = kek;
  }

  public RLong getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(RLong algorithm) {
    this.algorithm = algorithm;
  }

  public REnum<SecretType> getType() {
    return type;
  }

  public void setType(REnum<SecretType> type) {
    this.type = type;
  }

  public RString isUploadedFile() {
    return uploadedFile;
  }

  public void setUploadedFile(RString uploadedFile) {
    this.uploadedFile = uploadedFile;
  }
}
