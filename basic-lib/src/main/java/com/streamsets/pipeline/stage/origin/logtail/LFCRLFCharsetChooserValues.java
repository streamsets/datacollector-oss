/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.config.CharsetChooserValues;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Charset chooser-values that returns charsets that use single byte LF & CR as EOL characters.
 * <p/>
 * we accept only charsets that do so via the filter (see super-class for loop/filtering)
 * <p/>
 * we may be losing some charsets that have single byte LF & CR if the charset cannot encode (it is fro reading only).
 * this is because the test requires encoding \n and \r.
 * if we run into this situation we can explicitly whitelist such charsets.
 */
public class LFCRLFCharsetChooserValues extends CharsetChooserValues {

  private static class LFCRLFFilter implements Filter {
    @Override
    public boolean accept(Charset charset) {
      if (charset.canEncode()) {
        try {
          ByteBuffer bf = charset.encode("\n");
          if (bf.limit() != 1 || bf.get() != (byte) '\n') {
            return false;
          }
          bf = charset.encode("\r");
          if (bf.limit() != 1 || bf.get() != (byte) '\r') {
            return false;
          }
        } catch (Exception ex) {
          return false;
        }
      } else {
        return false;
      }
      return true;
    }
  }

  public LFCRLFCharsetChooserValues() {
    super(new LFCRLFFilter());
  }

}
