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
package com.streamsets.lib.security.http.aster;


import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAsterServiceProvider {

  @Test
  public void testEnabled() {
    Configuration configuration = new Configuration();
    Assert.assertFalse(AsterServiceProvider.isEnabled(configuration));

    configuration.set(AsterServiceProvider.ASTER_URL_CONF, "https://aster");
    Assert.assertTrue(AsterServiceProvider.isEnabled(configuration));
  }

  @Test
  public void testServiceInstance() {
    Assert.assertNull(AsterServiceProvider.getInstance().getService());

    AsterService service = Mockito.mock(AsterService.class);
    AsterServiceProvider.getInstance().set(service);
    Assert.assertNotNull(AsterServiceProvider.getInstance().getService());
    Assert.assertSame(service, AsterServiceProvider.getInstance().getService());

    AsterServiceProvider.getInstance().set(null);
    Assert.assertNull(AsterServiceProvider.getInstance().getService());
  }

}
