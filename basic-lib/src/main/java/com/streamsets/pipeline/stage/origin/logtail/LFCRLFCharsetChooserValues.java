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
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.config.CharsetChooserValues;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * <p>
 * Charset chooser-values that returns charsets that use single byte LF &amp; CR as EOL characters.
 * </p>
 * We accept only charsets that do so via the filter (see super-class for loop/filtering)
 * <p>
 * we may be losing some charsets that have single byte LF &amp; CR if the charset cannot encode (it is fro reading only).
 * this is because the test requires encoding \n and \r.
 * if we run into this situation we can explicitly whitelist such charsets.
 * </p>
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
