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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class Base64EL {
  @ElFunction(
      prefix = "base64",
      name = "encodeString",
      description = "Returns base64 encoded version of the string argument.")
  public static String base64Encode(
      @ElParam("string") String string, @ElParam("urlSafe") boolean urlSafe, @ElParam("charset") String charset
  ) throws UnsupportedEncodingException {
    return base64Encode(string.getBytes(charset), urlSafe);
  }

  @ElFunction(
      prefix = "base64",
      name = "encodeBytes",
      description = "Returns base64 encoded version of the string argument.")
  public static String base64Encode(@ElParam("string") byte[] bytes, @ElParam("urlSafe") boolean urlSafe) {
    if (urlSafe) {
      return Base64.encodeBase64URLSafeString(bytes);
    } else {
      return Base64.encodeBase64String(bytes);
    }
  }

  @ElFunction(
      prefix = "base64",
      name = "decodeString",
      description = "Returns a decoded string from a base64 encoded string argument and charset name.")
  public static String base64Decode(@ElParam("string") String encoded, @ElParam("charset") String charset) throws
      UnsupportedEncodingException {
    return new String(Base64.decodeBase64(encoded), Charset.forName(charset));
  }

  @ElFunction(
      prefix = "base64",
      name = "decodeBytes",
      description = "Returns a decoded byte array from a base64 encoded string argument.")
  public static byte[] base64Decode(@ElParam("string") String encoded) {
    return Base64.decodeBase64(encoded);
  }
}
