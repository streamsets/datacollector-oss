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
package com.streamsets.lib.security;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class SshUtils {

  // prevent instantiation
  private SshUtils() {}

  public static SshKeyInfoBean createSshKeyInfoBean(int len, String comment) {
    SshKeyInfoBean bean = new SshKeyInfoBean();
    KeyPair keys = createSshKeyPair(len);
    bean.password = UUID.randomUUID().toString();
    bean.privateKey = getSshPrivateKeyText(keys, bean.password);
    bean.publicKey = getSshPublicKeyText(keys, comment);
    return bean;
  }

  private static KeyPair createSshKeyPair(int len) {
    try {
      return KeyPair.genKeyPair(new JSch(), KeyPair.RSA, len);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String getSshPrivateKeyText(KeyPair keyPair, String password) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    keyPair.writePrivateKey(baos, password.getBytes(StandardCharsets.UTF_8));
    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String getSshPublicKeyText(KeyPair keyPair, String comment) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    keyPair.writePublicKey(baos, comment);
    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  }

  public static class SshKeyInfoBean {

    private String privateKey;
    private String publicKey;
    private String password;

    public String getPrivateKey() {
      return privateKey;
    }

    public void setPrivateKey(String privateKey) {
      this.privateKey = privateKey;
    }

    public String getPublicKey() {
      return publicKey;
    }

    public void setPublicKey(String publicKey) {
      this.publicKey = publicKey;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }
  }
}
