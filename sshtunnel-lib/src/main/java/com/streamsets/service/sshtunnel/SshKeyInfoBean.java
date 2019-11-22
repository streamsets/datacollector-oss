/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.service.sshtunnel;

/**
 * It is a data container class only, it does not validation on the data of its properties.
 */
public class SshKeyInfoBean {
  private String privateKey;
  private String publicKey;
  private String password;

  public String getPrivateKey() {
    return privateKey;
  }

  public SshKeyInfoBean setPrivateKey(String privateKey) {
    this.privateKey = privateKey;
    return this;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public SshKeyInfoBean setPublicKey(String publicKey) {
    this.publicKey = publicKey;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public SshKeyInfoBean setPassword(String password) {
    this.password = password;
    return this;
  }

  public boolean isValid() {
    return privateKey != null && publicKey != null && password != null;
  }

}
