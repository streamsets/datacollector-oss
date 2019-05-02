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
package com.streamsets.pipeline.stage.destination.s3;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

public class TestS3TargetSSEConfigBean {

  @Test
  public void testResolveAndEncodeEncryptionContextEmpty() throws Exception {
    S3TargetSSEConfigBean configBean = new S3TargetSSEConfigBean();
    configBean.encryptionContext = Collections.emptyList();
    Assert.assertNull(configBean.resolveAndEncodeEncryptionContext());

    configBean.encryptionContext = ImmutableList.of(makeEncryptionContextBean("", "empty-key"));
    Assert.assertNull(configBean.resolveAndEncodeEncryptionContext());
  }

  @Test
  public void testResolveAndEncodeEncryptionContext() throws Exception {
    S3TargetSSEConfigBean configBean = new S3TargetSSEConfigBean();
    configBean.encryptionContext = ImmutableList.of(
        makeEncryptionContextBean("x", "X"),
        makeEncryptionContextBean("", "empty-key"),
        makeEncryptionContextBean("y", "Y")
    );
    String encryptionContext = configBean.resolveAndEncodeEncryptionContext();
    Assert.assertNotNull(encryptionContext);
    Assert.assertNotNull(encryptionContext);
    String json = new String(Base64.getDecoder().decode(encryptionContext), StandardCharsets.UTF_8);
    Assert.assertEquals("{\"x\":\"X\",\"y\":\"Y\"}", json);
  }

  private EncryptionContextBean makeEncryptionContextBean(String key, String value) {
    EncryptionContextBean bean = new EncryptionContextBean();
    bean.key = key;
    bean.value = () -> value;
    return bean;
  }
}
