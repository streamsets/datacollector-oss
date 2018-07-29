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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class TestNopHttpRequestFragmenter {

  @Test
  public void testValidate() throws IOException {
    HttpRequestFragmenter fragmenter = new NopHttpRequestFragmenter();
    fragmenter.init(null);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertTrue(fragmenter.validate(req,res));
    Mockito.verifyNoMoreInteractions(req, res);
    fragmenter.destroy();
  }

  @Test
  public void testFragmentToFragmentInternal() throws Exception {
    NopHttpRequestFragmenter fragmenter = new NopHttpRequestFragmenter();
    fragmenter = Mockito.spy(fragmenter);
    List<byte[]> list = new ArrayList<>();
    Mockito
        .doReturn(list)
        .when(fragmenter)
        .fragmentInternal(Mockito.any(InputStream.class), Mockito.anyInt(), Mockito.anyInt());
    InputStream is = Mockito.mock(InputStream.class);

    fragmenter.init(null);

    Assert.assertEquals(list, fragmenter.fragment(is, 1, 2));
    Mockito.verify(fragmenter, Mockito.times(1)).fragmentInternal(Mockito.eq(is), Mockito.eq(1000), Mockito.eq(2000));
    fragmenter.destroy();
  }

  @Test
  public void testFragmenter() throws Exception {
    NopHttpRequestFragmenter fragmenter = new NopHttpRequestFragmenter();
    fragmenter.init(null);

    byte[] data = new byte[]{0, 1, 2, 3, 4};

    // under limits
    List<byte[]> list = fragmenter.fragment(new ByteArrayInputStream(data), 6, 6);
    Assert.assertEquals(1, list.size());
    Assert.assertArrayEquals(data, list.get(0));

    // on limits
    list = fragmenter.fragmentInternal(new ByteArrayInputStream(data), 5, 5);
    Assert.assertEquals(1, list.size());
    Assert.assertArrayEquals(data, list.get(0));

    fragmenter.destroy();
  }

  @Test(expected = IOException.class)
  public void testFragmenterWrongLimits() throws Exception {
    NopHttpRequestFragmenter fragmenter = new NopHttpRequestFragmenter();
    fragmenter.init(null);

    // wrongLimits
    fragmenter.fragmentInternal(null, 5, 6);
  }

  @Test(expected = IOException.class)
  public void testFragmenterOverLimits() throws Exception {
    NopHttpRequestFragmenter fragmenter = new NopHttpRequestFragmenter();
    fragmenter.init(null);

    byte[] data = new byte[]{0, 1, 2, 3, 4};

    // over limits
    fragmenter.fragmentInternal(new ByteArrayInputStream(data), 4, 4);
  }


}
