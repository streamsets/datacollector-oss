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
package com.streamsets.datacollector.credential.cyberark;

import com.streamsets.pipeline.api.credential.CredentialStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestConfiguration {

  @Test
  public void testConfiguration() {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig("s")).thenReturn("S");
    Mockito.when(context.getConfig("i")).thenReturn("1");
    Mockito.when(context.getConfig("l")).thenReturn("2");
    Mockito.when(context.getConfig("b")).thenReturn("true");

    Configuration configuration = new Configuration(context);
    Assert.assertEquals("id", configuration.getId());
    Assert.assertEquals("S", configuration.get("s", ""));
    Assert.assertEquals(1, configuration.get("i", 0));
    Assert.assertEquals(2L, configuration.get("l", 0L));
    Assert.assertEquals(true, configuration.get("b", false));
    Assert.assertEquals("S", configuration.get("ss", "S"));
    Assert.assertEquals(1, configuration.get("ii", 1));
    Assert.assertEquals(2L, configuration.get("ll", 2L));
    Assert.assertEquals(true, configuration.get("bb", true));
  }
}
