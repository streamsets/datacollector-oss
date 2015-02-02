/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.base.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

public class TestOverrunReader {

  @Test
  public void testOverrunUnderLimit() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64, true);
    char[] buff = new char[128];
    ois.read(buff, 0, 64);
    ois.resetCount();
    ois.read(buff, 0, 64);
  }

  @Test(expected = OverrunException.class)
  public void testOverrunOverLimit() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64, true);
    char[] buff = new char[128];
    ois.read(buff, 0, 65);
  }

  @Test
  public void testOverrunOverLimitNotEnabled() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64, false);
    char[] buff = new char[128];
    ois.read(buff, 0, 65);
  }

  @Test(expected =  OverrunException.class)
  public void testOverrunOverLimitPostConstructorEnabled() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 1280));
    OverrunReader ois = new OverrunReader(is, 64, false);
    char[] buff = new char[128];
    try {
      ois.read(buff, 0, 65);
    } catch (OverrunException ex) {
      Assert.fail();
    }
    ois.setEnabled(true);
    buff = new char[128];
    ois.read(buff, 0, 65);
  }

}
