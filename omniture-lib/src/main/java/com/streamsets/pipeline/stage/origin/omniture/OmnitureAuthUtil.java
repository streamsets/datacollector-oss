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
package com.streamsets.pipeline.stage.origin.omniture;

import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility methods for generating an Omniture WSSE header
 * https://marketing.adobe.com/developer/documentation/authentication-1/using-web-service-credentials-2
 */
public class OmnitureAuthUtil {
  private static final String SHA1 = "SHA-1";
  private static final String AUTH_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  private static BASE64Encoder b64 = new BASE64Encoder();

  private OmnitureAuthUtil() {}

  public static String getHeader(String clientId, String clientSecret) throws UnsupportedEncodingException {
    byte[] nonceB = generateNonce();
    String nonce = base64Encode(nonceB);
    String created = generateTimestamp();
    String password64 = getBase64Digest(nonceB, created.getBytes("UTF-8"), clientSecret.getBytes("UTF-8"));
    return "UsernameToken Username=\"" +
        clientId +
        "\", " +
        "PasswordDigest=\"" +
        password64.trim() +
        "\", " +
        "Nonce=\"" +
        nonce.trim() +
        "\", " +
        "Created=\"" +
        created +
        "\"";
  }

  private static byte[] generateNonce() {
    String nonce = Long.toString(new Date().getTime());
    return nonce.getBytes(StandardCharsets.UTF_8);
  }

  private static String generateTimestamp() {
    SimpleDateFormat dateFormatter = new SimpleDateFormat(AUTH_DATE_FORMAT);
    dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dateFormatter.format(new Date());
  }

  private static synchronized String getBase64Digest(byte[] nonce, byte[] created, byte[] password) {
    try {
      MessageDigest messageDigester = MessageDigest.getInstance(SHA1);
      // SHA-1 ( nonce + created + password )
      messageDigester.reset();
      messageDigester.update(nonce);
      messageDigester.update(created);
      messageDigester.update(password);
      return base64Encode(messageDigester.digest());
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static String base64Encode(byte[] bytes) {
    return b64.encode(bytes);
  }
}
