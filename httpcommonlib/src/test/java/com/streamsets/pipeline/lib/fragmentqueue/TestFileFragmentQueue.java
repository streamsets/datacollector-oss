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
package com.streamsets.pipeline.lib.fragmentqueue;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestFileFragmentQueue {

  private byte[] fill(byte[] array, byte value) {
    for (int i = 0; i < array.length; i++) {
      array[i] = value;;
    }
    return array;
  }

  @Test
  public void testFileFragmentQueue() throws Exception {
    FileFragmentQueue ffq = new FileFragmentQueue(1);
    ffq = Mockito.spy(ffq);
    Mockito.doReturn(1000L).when(ffq).getMaxQueueFileSize(); // faking max size down to 1KB

    ffq.init(ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, ImmutableList.of("a")));

    Assert.assertNull(ffq.poll(4));

    byte[] fragment1 = fill(new byte[100], (byte) 1);
    byte[] fragment2 = fill(new byte[200], (byte) 2);
    byte[] fragment3 = fill(new byte[1000], (byte) 3); //this fragment will be lost
    byte[] fragment4 = fill(new byte[100], (byte) 4);
    List<byte[]> fragments = ImmutableList.of(fragment1, fragment2, fragment3, fragment4);

    Assert.assertEquals(0, ffq.getLostFragmentsCountAndReset());

    ffq.write(fragments);
    List<byte[]> got = ffq.poll(4);
    Assert.assertEquals(3, got.size());
    Assert.assertArrayEquals(fragment1, got.get(0));
    Assert.assertArrayEquals(fragment2, got.get(1));
    Assert.assertArrayEquals(fragment4, got.get(2));

    Assert.assertEquals(1, ffq.getLostFragmentsCountAndReset());
    Assert.assertEquals(0, ffq.getLostFragmentsCountAndReset());

    long start = System.currentTimeMillis();
    Assert.assertNull(ffq.poll(1, 100));
    Assert.assertTrue(System.currentTimeMillis() - start >= 100);

    ffq.destroy();
  }
}
