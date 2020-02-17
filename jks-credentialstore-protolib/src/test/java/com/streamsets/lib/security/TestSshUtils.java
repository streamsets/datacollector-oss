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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;


public class TestSshUtils {

  @Test
  public void testCreateSshKeyInfoBean() {
    String comment = "testCreateSshKeyInfoBean";
    SshUtils.SshKeyInfoBean bean = SshUtils.createSshKeyInfoBean(4096, comment);
    Assert.assertTrue(bean.getPublicKey().length() > 512);
    Assert.assertTrue(bean.getPublicKey().contains(comment));
    Assert.assertTrue(bean.getPassword().length() > 16);
    Assert.assertTrue(bean.getPrivateKey().length() > 1024);

    SshUtils.SshKeyInfoBean shortLengthBean = SshUtils.createSshKeyInfoBean(4096 / 4, comment);
    Assert.assertTrue(shortLengthBean.getPublicKey().length() > 512 / 4);
    Assert.assertTrue(shortLengthBean.getPublicKey().length() < bean.getPublicKey().length());
    Assert.assertTrue(shortLengthBean.getPublicKey().contains(comment));
    Assert.assertTrue(shortLengthBean.getPassword().length() > 16);
    Assert.assertTrue(shortLengthBean.getPrivateKey().length() > 1024 / 4);
    Assert.assertTrue(shortLengthBean.getPrivateKey().length() < bean.getPrivateKey().length());
  }

  @Test
  public void testSshKeyInfoBeanSerialization() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();

    String comment = "testSerialization";

    SshUtils.SshKeyInfoBean bean = SshUtils.createSshKeyInfoBean(4096, comment);

    String serializedBean = objectMapper.writeValueAsString(bean);
    Assert.assertFalse(serializedBean.trim().isEmpty());
    Assert.assertTrue(serializedBean.length() > 1024);

    SshUtils.SshKeyInfoBean deserializedBean = objectMapper.readValue(serializedBean, SshUtils.SshKeyInfoBean.class);
    Assert.assertEquals(bean.getPublicKey(), deserializedBean.getPublicKey());
    Assert.assertEquals(bean.getPassword(), deserializedBean.getPassword());
    Assert.assertEquals(bean.getPrivateKey(), deserializedBean.getPrivateKey());
  }
}