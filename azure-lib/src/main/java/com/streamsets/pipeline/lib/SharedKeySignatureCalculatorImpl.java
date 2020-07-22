/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.lib;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Calculates an Azure Shared Key Signature.
 * See https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key for info of Shared Key
 * Signature calculation
 */
public class SharedKeySignatureCalculatorImpl implements SharedKeySignatureCalculator {

  private static final String EMPTY_FIELD = "";

  public static class Builder {
    private String verb;
    private String contentEncoding;
    private String contentLanguage;
    private String contentLength;
    private String contentMD5;
    private String contentType;
    private String date;
    private String ifModifiedSince;
    private String ifMatch;
    private String ifNoneMatch;
    private String ifUnmodifiedSince;
    private String range;
    private String canonicalizedHeaders;
    private String canonicalizedResource;

    public Builder() {
      this.verb = EMPTY_FIELD;
      this.contentEncoding = EMPTY_FIELD;
      this.contentLanguage = EMPTY_FIELD;
      this.contentLength = EMPTY_FIELD;
      this.contentMD5 = EMPTY_FIELD;
      this.contentType = EMPTY_FIELD;
      this.date = EMPTY_FIELD;
      this.ifModifiedSince = EMPTY_FIELD;
      this.ifMatch = EMPTY_FIELD;
      this.ifNoneMatch = EMPTY_FIELD;
      this.ifUnmodifiedSince = EMPTY_FIELD;
      this.range = EMPTY_FIELD;
      this.canonicalizedHeaders = EMPTY_FIELD;
      this.canonicalizedResource = EMPTY_FIELD;
    }

    public void setVerb(String verb) {
      this.verb = verb;
    }

    public void setContentEncoding(String contentEncoding) {
      this.contentEncoding = contentEncoding;
    }

    public void setContentLanguage(String contentLanguage) {
      this.contentLanguage = contentLanguage;
    }

    public void setContentLength(String contentLength) {
      this.contentLength = contentLength;
    }

    public void setContentMD5(String contentMD5) {
      this.contentMD5 = contentMD5;
    }

    public void setContentType(String contentType) {
      this.contentType = contentType;
    }

    public void setDate(String date) {
      this.date = date;
    }

    public void setIfModifiedSince(String ifModifiedSince) {
      this.ifModifiedSince = ifModifiedSince;
    }

    public void setIfMatch(String ifMatch) {
      this.ifMatch = ifMatch;
    }

    public void setIfNoneMatch(String ifNoneMatch) {
      this.ifNoneMatch = ifNoneMatch;
    }

    public void setIfUnmodifiedSince(String ifUnmodifiedSince) {
      this.ifUnmodifiedSince = ifUnmodifiedSince;
    }

    public void setRange(String range) {
      this.range = range;
    }

    public void setCanonicalizedHeaders(String canonicalizedHeaders) {
      this.canonicalizedHeaders = canonicalizedHeaders;
    }

    public void setCanonicalizedResource(String canonicalizedResource) {
      this.canonicalizedResource = canonicalizedResource;
    }

    public SharedKeySignatureCalculatorImpl build() {
      Preconditions.checkState(StringUtils.isNotEmpty(verb));
      return new SharedKeySignatureCalculatorImpl(
          verb,
          contentEncoding,
          contentLanguage,
          contentLength,
          contentMD5,
          contentType,
          date,
          ifModifiedSince,
          ifMatch,
          ifNoneMatch,
          ifUnmodifiedSince,
          range,
          canonicalizedHeaders,
          canonicalizedResource
      );
    }
  }

  private final static String HMAC_SHA256 = "HmacSHA256";

  private final String verb;
  private final String contentEncoding;
  private final String contentLanguage;
  private final String contentLength;
  private final String contentMD5;
  private final String contentType;
  private final String date;
  private final String ifModifiedSince;
  private final String ifMatch;
  private final String ifNoneMatch;
  private final String ifUnmodifiedSince;
  private final String range;
  private final String canonicalizedHeaders;
  private final String canonicalizedResource;

  public SharedKeySignatureCalculatorImpl(
      String verb,
      String contentEncoding,
      String contentLanguage,
      String contentLength,
      String contentMD5,
      String contentType,
      String date,
      String ifModifiedSince,
      String ifMatch,
      String ifNoneMatch,
      String ifUnmodifiedSince,
      String range,
      String canonicalizedHeaders,
      String canonicalizedResource
  ) {
    this.verb = verb;
    this.contentEncoding = contentEncoding;
    this.contentLanguage = contentLanguage;
    this.contentLength = contentLength;
    this.contentMD5 = contentMD5;
    this.contentType = contentType;
    this.date = date;
    this.ifModifiedSince = ifModifiedSince;
    this.ifMatch = ifMatch;
    this.ifNoneMatch = ifNoneMatch;
    this.ifUnmodifiedSince = ifUnmodifiedSince;
    this.range = range;
    this.canonicalizedHeaders = canonicalizedHeaders;
    this.canonicalizedResource = canonicalizedResource;
  }

  @Override
  public String getSignature(String key) throws InvalidKeyException, NoSuchAlgorithmException {
    String stringToSign = getStringToSign();
    byte[] hmacSha256 = performHmacSha256(stringToSign, key);
    return Base64.getEncoder().encodeToString(hmacSha256);
  }

  private String getStringToSign() {
    return String.format(
        "%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s%s",
        verb,
        contentEncoding,
        contentLanguage,
        contentLength,
        contentMD5,
        contentType,
        date,
        ifModifiedSince,
        ifMatch,
        ifNoneMatch,
        ifUnmodifiedSince,
        range,
        canonicalizedHeaders,
        canonicalizedResource
    );
  }

  private byte[] performHmacSha256(String stringToSign, String key)
      throws NoSuchAlgorithmException, InvalidKeyException {
    Charset charset = StandardCharsets.UTF_8;
    Mac hmacSha256 = Mac.getInstance(HMAC_SHA256);
    SecretKeySpec secretKeySpec = new SecretKeySpec(Base64.getDecoder().decode(key), HMAC_SHA256);
    hmacSha256.init(secretKeySpec);
    return hmacSha256.doFinal(charset.encode(stringToSign).array());
  }

}
