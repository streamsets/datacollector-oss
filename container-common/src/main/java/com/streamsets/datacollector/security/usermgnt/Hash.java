/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security.usermgnt;

import com.streamsets.pipeline.api.impl.Utils;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

  public static String getMd5Hash(String value) {
    Utils.checkNotNull(value, "value");
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(value.getBytes());
      byte[] digest = md.digest();
      return DatatypeConverter.printHexBinary(digest).toUpperCase();
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("Should not happen", ex);
    }
  }

}
