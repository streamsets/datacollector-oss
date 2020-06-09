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
package com.streamsets.pipeline.lib.http;

import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestHttpReceiver {

  @Test
  public void testReceiver() throws Exception {
    HttpConfigs configs = Mockito.mock(HttpConfigs.class);
    List id = new ArrayList<>(Arrays.asList(new CredentialValueBean("id")));
    Mockito.when(configs.getAppIds()).thenReturn(id);
    HttpRequestFragmenter fragmenter = Mockito.mock(HttpRequestFragmenter.class);
    FragmentWriter writer = Mockito.mock(FragmentWriter.class);
    HttpReceiver receiver = new HttpReceiverWithFragmenterWriter("/", configs, fragmenter, writer);

    Assert.assertEquals("/", receiver.getUriPath());

    Assert.assertEquals("id", receiver.getAppIds().get(0).get());

    Mockito
        .when(fragmenter.validate(Mockito.any(HttpServletRequest.class), Mockito.any(HttpServletResponse.class)))
        .thenReturn(true);
    Assert.assertTrue(receiver.validate(null, null));


    Mockito.when(configs.getMaxHttpRequestSizeKB()).thenReturn(2);
    Mockito.when(writer.getMaxFragmentSizeKB()).thenReturn(1);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    InputStream is = Mockito.mock(InputStream.class);
    List<byte[]> fragments = new ArrayList<>();
    Mockito.when(fragmenter.fragment(Mockito.eq(is), Mockito.eq(1), Mockito.eq(2))).thenReturn(fragments);

    receiver.process(req, is, null);
    Mockito.verify(fragmenter, Mockito.times(1)).fragment(Mockito.eq(is), Mockito.eq(1), Mockito.eq(2));
    Mockito.verify(writer, Mockito.times(1)).write(Mockito.eq(fragments));

  }
}
