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
package com.streamsets.datacollector.client;

import com.streamsets.datacollector.client.auth.Authentication;
import com.streamsets.datacollector.client.auth.HttpBasicAuth;
import com.streamsets.datacollector.client.auth.HttpDPMAuth;
import com.streamsets.datacollector.client.auth.HttpDigestAuth;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.glassfish.jersey.logging.LoggingFeature;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ApiClient {
  private Map<String, Client> hostMap = new HashMap<>();
  private Map<String, String> defaultHeaderMap = new HashMap<>();
  private boolean debugging = false;
  private String basePath = "https://localhost/rest";
  private JSON json = new JSON();
  private SSLContext sslContext = null;

  private final Authentication authentication;

  private int statusCode;
  private MultivaluedMap<String, Object> responseHeaders;

  private DateFormat dateFormat;

  public ApiClient() {
    this("basic");
  }

  public ApiClient(String authType) {
    // Use ISO 8601 format for date and datetime.
    // See https://en.wikipedia.org/wiki/ISO_8601
    this.dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    // Use UTC as the default time zone.
    this.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    // Set default User-Agent.
    setUserAgent("Java-Swagger");

    switch(authType) {
      case "basic":
      case "form":
        authentication = new HttpBasicAuth();
        break;
      case "digest":
        authentication = new HttpDigestAuth();
        break;
      case "dpm":
        authentication = new HttpDPMAuth();
        break;
      default:
        authentication = null;
    }
  }

  public String getBasePath() {
    return basePath;
  }

  public ApiClient setBasePath(String basePath) {
    this.basePath = basePath;
    return this;
  }

  /**
   * Gets the status code of the previous request
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * Gets the response headers of the previous request
   */
  public MultivaluedMap<String, Object> getResponseHeaders() {
    return responseHeaders;
  }

  /**
   * Helper method to set username for the first HTTP basic authentication.
   */
  public ApiClient setUsername(String username) {
    if (authentication != null) {
      authentication.setUsername(username);
    }
    return this;
  }

  /**
   * Helper method to set password for the first HTTP basic authentication.
   */
  public ApiClient setPassword(String password) {
    if (authentication != null) {
      authentication.setPassword(password);
    }
    return this;
  }

  /**
   * Helper method to set dpmBaseURL for the first HTTP DPM authentication.
   */
  public ApiClient setDPMBaseURL(String dpmBaseURL) {
    if (dpmBaseURL != null && authentication != null) {
      authentication.setDPMBaseURL(dpmBaseURL);
    }
    return this;
  }

  /**
   * Set the User-Agent header's value (by adding to the default header map).
   */
  public ApiClient setUserAgent(String userAgent) {
    addDefaultHeader("User-Agent", userAgent);
    return this;
  }

  /**
   * Add a default header.
   *
   * @param key The header's key
   * @param value The header's value
   */
  public ApiClient addDefaultHeader(String key, String value) {
    defaultHeaderMap.put(key, value);
    return this;
  }

  /**
   * Check that whether debugging is enabled for this API client.
   */
  public boolean isDebugging() {
    return debugging;
  }

  /**
   * Enable/disable debugging for this API client.
   *
   * @param debugging To enable (true) or disable (false) debugging
   */
  public ApiClient setDebugging(boolean debugging) {
    this.debugging = debugging;
    return this;
  }

  /**
   * Get the date format used to parse/format date parameters.
   */
  public DateFormat getDateFormat() {
    return dateFormat;
  }

  /**
   * Set the date format used to parse/format date parameters.
   */
  public ApiClient getDateFormat(DateFormat dateFormat) {
    this.dateFormat = dateFormat;
    return this;
  }

  /**
   * Parse the given string into Date object.
   */
  public Date parseDate(String str) {
    try {
      return dateFormat.parse(str);
    } catch (java.text.ParseException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Format the given Date object into string.
   */
  public String formatDate(Date date) {
    return dateFormat.format(date);
  }

  /**
   * Format the given parameter object into string.
   */
  public String parameterToString(Object param) {
    if (param == null) {
      return "";
    } else if (param instanceof Date) {
      return formatDate((Date) param);
    } else if (param instanceof Collection) {
      StringBuilder b = new StringBuilder();
      for(Object o : (Collection)param) {
        if (b.length() > 0) {
          b.append(",");
        }
        b.append(String.valueOf(o));
      }
      return b.toString();
    } else {
      return String.valueOf(param);
    }
  }

  /*
    Format to {@code Pair} objects.
  */
  public List<Pair> parameterToPairs(String collectionFormat, String name, Object value){
    List<Pair> params = new ArrayList<Pair>();

    // preconditions
    if (name == null || name.isEmpty() || value == null) return params;

    Collection valueCollection = null;
    if (value instanceof Collection) {
      valueCollection = (Collection) value;
    } else {
      params.add(new Pair(name, parameterToString(value)));
      return params;
    }

    if (valueCollection.isEmpty()){
      return params;
    }

    // get the collection format
    collectionFormat = (collectionFormat == null || collectionFormat.isEmpty() ? "csv" : collectionFormat); // default: csv

    // create the params based on the collection format
    if (collectionFormat.equals("multi")) {
      for (Object item : valueCollection) {
        params.add(new Pair(name, parameterToString(item)));
      }

      return params;
    }

    String delimiter = ",";

    if (collectionFormat.equals("csv")) {
      delimiter = ",";
    } else if (collectionFormat.equals("ssv")) {
      delimiter = " ";
    } else if (collectionFormat.equals("tsv")) {
      delimiter = "\t";
    } else if (collectionFormat.equals("pipes")) {
      delimiter = "|";
    }

    StringBuilder sb = new StringBuilder() ;
    for (Object item : valueCollection) {
      sb.append(delimiter);
      sb.append(parameterToString(item));
    }

    params.add(new Pair(name, sb.substring(1)));

    return params;
  }

  /**
   * Select the Accept header's value from the given accepts array:
   *   if JSON exists in the given array, use it;
   *   otherwise use all of them (joining into a string)
   *
   * @param accepts The accepts array to select from
   * @return The Accept header to use. If the given array is empty,
   *   null will be returned (not to set the Accept header explicitly).
   */
  public String selectHeaderAccept(String[] accepts) {
    if (accepts.length == 0) return null;
    if (StringUtil.containsIgnoreCase(accepts, "application/json")) return "application/json";
    return StringUtil.join(accepts, ",");
  }

  /**
   * Select the Content-Type header's value from the given array:
   *   if JSON exists in the given array, use it;
   *   otherwise use the first one of the array.
   *
   * @param contentTypes The Content-Type array to select from
   * @return The Content-Type header to use. If the given array is empty,
   *   JSON will be used.
   */
  public String selectHeaderContentType(String[] contentTypes) {
    if (contentTypes.length == 0) return "application/json";
    if (StringUtil.containsIgnoreCase(contentTypes, "application/json")) return "application/json";
    return contentTypes[0];
  }

  /**
   * Escape the given string to be used as URL query value.
   */
  public String escapeString(String str) {
    try {
      return URLEncoder.encode(str, "utf8").replaceAll("\\+", "%20");
    } catch (UnsupportedEncodingException e) {
      return str;
    }
  }

  /**
   * Serialize the given Java object into string according the given
   * Content-Type (only JSON is supported for now).
   */
  public String serialize(Object obj, String contentType) throws ApiException {
    if (contentType.startsWith("application/json")) {
      return json.serialize(obj);
    } else {
      throw new ApiException(400, "can not serialize object into Content-Type: " + contentType);
    }
  }

  /**
   * Deserialize response body to Java object according to the Content-Type.
   */
  private <T> T deserialize(Response response, TypeRef returnType) throws ApiException {
    String contentType = null;
    List<Object> contentTypes = response.getHeaders().get("Content-Type");
    if (contentTypes != null && !contentTypes.isEmpty()) {
      contentType = (String)contentTypes.get(0);
    }

    if (contentType == null) {
      throw new ApiException(500, "missing Content-Type in response");
    }

    if (contentType.startsWith("application/json")) {
      String body;
      if (response.hasEntity()) {
        body = response.readEntity(String.class);
      } else {
        body = "";
      }
      if (body.length() > 0) {
        return json.deserialize(body, returnType);
      }
      return null;
    } if (contentType.startsWith("image")) {
      return (T) response.readEntity(InputStream.class);
    } else {
      throw new ApiException(500, "can not deserialize Content-Type: " + contentType);
    }
  }

  private Response getAPIResponse(
      String path,
      String method,
      List<Pair> queryParams,
      Object body,
      byte[] binaryBody,
      Map<String, String> headerParams,
      Map<String, Object> formParams,
      String accept,
      String contentType,
      String[] authNames
  ) throws ApiException {
    String userAuthToken = null;
    try {
      if (body != null && binaryBody != null){
        throw new ApiException(500, "either body or binaryBody must be null");
      }

      Client client = getClient();
      Response response = null;

      StringBuilder b = new StringBuilder();
      b.append("?");
      if (queryParams != null){
        for (Pair queryParam : queryParams){
          if (!queryParam.getName().isEmpty()) {
            b.append(escapeString(queryParam.getName()));
            b.append("=");
            b.append(escapeString(queryParam.getValue()));
            b.append("&");
          }
        }
      }

      String querystring = b.substring(0, b.length() - 1);

      WebTarget target = client.target(basePath + path + querystring);

      if (debugging) {
        Logger logger = Logger.getLogger(getClass().getName());
        Feature loggingFeature = new LoggingFeature(logger, Level.INFO, null, null);
        target.register(loggingFeature);
      }

      if (authentication != null) {
        userAuthToken = authentication.login();
        authentication.setFilter(target);
      }

      Invocation.Builder builder;
      if (accept != null) {
        builder = target.request(accept);
      } else {
        builder = target.request();
      }

      for (Map.Entry<String, String> entry : headerParams.entrySet()) {
        builder = builder.header(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, String> entry : defaultHeaderMap.entrySet()) {
        if (!headerParams.containsKey(entry.getKey())) {
          builder = builder.header(entry.getKey(), entry.getValue());
        }
      }

      if (authentication != null) {
        authentication.setHeader(builder, userAuthToken);
      }

      if ("GET".equals(method)) {
        response = builder.get();
      } else if ("POST".equals(method)) {
        response = builder.post(Entity.entity(body, contentType));
      } else if ("PUT".equals(method)) {
        response = builder.put(Entity.entity(body, contentType));
      } else if ("DELETE".equals(method)) {
        response = builder.delete();
      } else {
        throw new ApiException(500, "unknown method type " + method);
      }

      return response;
    } finally {
      if (authentication != null && userAuthToken != null) {
        authentication.logout(userAuthToken);
      }
    }
  }

  /**
   * Invoke API by sending HTTP request with the given options.
   *
   * @param path The sub-path of the HTTP URL
   * @param method The request method, one of "GET", "POST", "PUT", and "DELETE"
   * @param queryParams The query parameters
   * @param body The request body object - if it is not binary, otherwise null
   * @param binaryBody The request body object - if it is binary, otherwise null
   * @param headerParams The header parameters
   * @param formParams The form parameters
   * @param accept The request's Accept header
   * @param contentType The request's Content-Type header
   * @param authNames The authentications to apply
   * @return The response body in type of string
   */
  public <T> T invokeAPI(String path, String method, List<Pair> queryParams, Object body, byte[] binaryBody,
                         Map<String, String> headerParams, Map<String, Object> formParams, String accept,
                         String contentType, String[] authNames, TypeRef returnType) throws ApiException {

    Response response = getAPIResponse(path, method, queryParams, body, binaryBody, headerParams, formParams,
        accept, contentType, authNames);

    statusCode = response.getStatusInfo().getStatusCode();
    responseHeaders = response.getHeaders();

    if (statusCode == 401) {
      throw new ApiException(
          response.getStatusInfo().getStatusCode(),
          "HTTP Error 401 - Unauthorized: Access is denied due to invalid credentials.",
          response.getHeaders(),
          null);
    } else if (response.getStatusInfo() == Response.Status.NO_CONTENT) {
      return null;
    } else if (response.getStatusInfo().getFamily() == Family.SUCCESSFUL) {
      if (returnType == null)
        return null;
      else
        return deserialize(response, returnType);
    } else {
      String message = "error";
      String respBody = null;
      if (response.hasEntity()) {
        try {
          respBody = response.readEntity(String.class);
          message = respBody;
        } catch (RuntimeException e) {
          // e.printStackTrace();
        }
      }
      throw new ApiException(
          response.getStatusInfo().getStatusCode(),
          message,
          response.getHeaders(),
          respBody);
    }
  }

  /**
   * Encode the given form parameters as request body.
   */
  private String getXWWWFormUrlencodedParams(Map<String, Object> formParams) {
    StringBuilder formParamBuilder = new StringBuilder();

    for (Entry<String, Object> param : formParams.entrySet()) {
      String keyStr = param.getKey();
      String valueStr = parameterToString(param.getValue());
      try {
        formParamBuilder.append(URLEncoder.encode(keyStr, "utf8"))
            .append("=")
            .append(URLEncoder.encode(valueStr, "utf8"));
        formParamBuilder.append("&");
      } catch (UnsupportedEncodingException e) {
        // move on to next
      }
    }

    String encodedFormParams = formParamBuilder.toString();
    if (encodedFormParams.endsWith("&")) {
      encodedFormParams = encodedFormParams.substring(0, encodedFormParams.length() - 1);
    }

    return encodedFormParams;
  }

  public void setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
  }

  /**
   * Get an existing client or create a new client to handle HTTP request.
   */
  private Client getClient() {
    if (!hostMap.containsKey(basePath)) {
      ClientConfig config = new ClientConfig();
      config.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
      Client client;
      if (sslContext != null) {
        client = ClientBuilder.newBuilder().sslContext(sslContext).withConfig(config).build();
      } else {
        client = ClientBuilder.newClient(config);
      }
      client.register(new CsrfProtectionFilter("CSRF"));
      hostMap.put(basePath, client);
    }
    return hostMap.get(basePath);
  }

  public JSON getJson() {
    return json;
  }
}
