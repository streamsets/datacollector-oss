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

import com.streamsets.pipeline.api.Stage;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class AzureUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AzureUtils.class);

  private static final String REQUEST_TYPE_PATCH = "PATCH";
  private static final String PARTNER_ID = "89353133-d656-5f71-98e4-4b55bb468a02";
  private static final String AUTHORIZATION_VERSION = "2017-11-09";
  private static final String PARTNER_TAGGING_URI_PATH = "/names/0";
  private static final String PARTNER_TAGGING_COMP = "partners";
  private static final String PARTNER_TAGGING_RESTYPE = "service";

  public static final String ADLS_USER_AGENT_STRING_KEY = "fs.azure.user.agent.prefix";

  public static final String OAUTH_AUTHORIZATION = "Bearer %s";

  private AzureUtils() {
    //Empty constructor hiding implicit public one
  }

  public static String buildUserAgentString(Stage.Context context) {
    return String.format("APN/1.0 streamsets/1.0 datacollector/%s", context.getEnvironmentVersion());
  }


  /**
   * Sends the HTTP request to the partner tagging including the SDC PARTNER_ID
   *
   * @param accountFQDN Fully qualified domain name account
   * @param key Shared key, depending on which is the auth mode
   */
  public static void sendPartnerTaggingRequest(String accountFQDN, String key) {
    sendPartnerTaggingRequest(accountFQDN, key, false);
  }

  /**
   * Sends the HTTP request to the partner tagging including the SDC PARTNER_ID
   *
   * @param accountFQDN Fully qualified domain name account
   * @param key Shared key or OAuth token, depending on which is the auth mode
   * @param isOauth is OAuth authentication or Shared Key
   */
  public static void sendPartnerTaggingRequest(String accountFQDN, String key, boolean isOauth) {
    //Change from DFS to BLOB since the query is specific to BLOB
    accountFQDN = accountFQDN.replace(".dfs.core.windows.net", ".blob.core.windows.net");

    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      LOG.debug("Sending partner tagging request");

      // prepare date formatting
      DateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss");
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      String xMsDate = dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()).concat(" GMT");

      String authKey;

      if (isOauth) {
        authKey = String.format(OAUTH_AUTHORIZATION, key);
      } else {
        String canonicalizedHeaders = getCanonicalizedHeaders(xMsDate);
        String canonicalizedResource = getCanonicalizedResource(accountFQDN);
        authKey = String.format("SharedKey %s:%s",
            accountFQDN,
            getSharedKeySignature(key, canonicalizedHeaders, canonicalizedResource)
        );

      }

      HttpUriRequest request = RequestBuilder.create(REQUEST_TYPE_PATCH)
                                             .setUri(String.format("https://%s/?restype=%s&comp=%s",
                                                 accountFQDN,
                                                 PARTNER_TAGGING_RESTYPE,
                                                 PARTNER_TAGGING_COMP
                                             ))
                                             .setEntity(new StringEntity("{\"op\": \"add\", \"path\": \"/names/0\", " +
                                                 "\"value\": " +
                                                 "\"streamsets\"}", ContentType.APPLICATION_JSON))
                                             .addHeader("x-ms-date", xMsDate)
                                             .addHeader("x-ms-partner-id", PARTNER_ID)
                                             .addHeader("x-ms-version", AUTHORIZATION_VERSION)
                                             .addHeader("Authorization", authKey)
                                             .build();

      HttpResponse response = httpClient.execute(request);

      // check response
      if (response.getStatusLine().getStatusCode() == 202) {
        LOG.debug("Successfully performed partner tagging request");
      } else {
        LOG.warn("Failed to execute partner tagging request with status: {}. Reason: {}",
            response.getStatusLine().getStatusCode(),
            response.getStatusLine().getReasonPhrase()
        );
      }
    } catch (Exception e) {
      //Catching generic exception to avoid partner tagging have an impact in the pipeline execution
      LOG.warn("Exception thrown when sending partner tagging request.", e);
    }
  }

  private static String getCanonicalizedHeaders(String xMsDate) {
    return String.format("x-ms-date:%s%nx-ms-partner-id:%s%nx-ms-version:%s%n",
        xMsDate,
        AzureUtils.PARTNER_ID,
        AzureUtils.AUTHORIZATION_VERSION
    );
  }

  private static String getCanonicalizedResource(String accountName) {
    return String.format("/%s%s\ncomp:%s\nrestype:%s\n",
        accountName,
        AzureUtils.PARTNER_TAGGING_URI_PATH,
        AzureUtils.PARTNER_TAGGING_COMP,
        AzureUtils.PARTNER_TAGGING_RESTYPE
    );
  }

  private static String getSharedKeySignature(
      String accountKey, String canonicalizedHeaders, String canonicalizedResource
  ) throws NoSuchAlgorithmException, InvalidKeyException {
    SharedKeySignatureCalculatorImpl.Builder sharedKeySignatureBuilder = new SharedKeySignatureCalculatorImpl.Builder();
    sharedKeySignatureBuilder.setVerb(AzureUtils.REQUEST_TYPE_PATCH);
    sharedKeySignatureBuilder.setCanonicalizedHeaders(canonicalizedHeaders);
    sharedKeySignatureBuilder.setCanonicalizedResource(canonicalizedResource);
    SharedKeySignatureCalculator sharedKeySignature = sharedKeySignatureBuilder.build();
    return sharedKeySignature.getSignature(accountKey);
  }

}
