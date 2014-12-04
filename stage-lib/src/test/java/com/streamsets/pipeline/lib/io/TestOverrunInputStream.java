/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.base.Strings;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class TestOverrunInputStream {

  @Test
  public void testOverrunUnderLimit() throws Exception {
    InputStream is = new ByteArrayInputStream(Strings.repeat("a", 128).getBytes());
    OverrunInputStream ois = new OverrunInputStream(is, 64);
    byte[] buff = new byte[128];
    ois.read(buff, 0, 64);
    ois.resetByteCount();
    ois.read(buff, 0, 64);
  }

  @Test(expected = OverrunException.class)
  public void testOverrunOverLimit() throws Exception {
    InputStream is = new ByteArrayInputStream(Strings.repeat("a", 128).getBytes());
    OverrunInputStream ois = new OverrunInputStream(is, 64);
    byte[] buff = new byte[128];
    ois.read(buff, 0, 65);
  }

}
