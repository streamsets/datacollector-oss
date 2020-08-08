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
package com.streamsets.datacollector.client.api;

import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.Configuration;
import com.streamsets.datacollector.client.Pair;
import com.streamsets.datacollector.client.TypeRef;
import com.streamsets.datacollector.client.model.DPMInfoJson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemApi {
  private ApiClient apiClient;

  public SystemApi() {
    this(Configuration.getDefaultApiClient());
  }

  public SystemApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }


  /**
   * Returns ALL SDC Configuration
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getConfiguration() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/configuration".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns UI SDC Configuration
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getUIConfiguration() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/configuration/ui".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns SDC Directories
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getSDCDirectories() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/directories".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns SDC Info
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getBuildInfo() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/info".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns User Info
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getUserInfo() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/info/currentUser".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns Server Time
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getServerTime() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/info/serverTime".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns Stats
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getStats() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/stats".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Map<String, Object>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Return latest log file contents
   *
   * @param endingOffset
   * @return void
   */
  public void currentLog(Long endingOffset) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/logs".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "endingOffset", endingOffset));

    final String[] accepts = {
      "text/plain"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] {  };

    apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);
  }

  /**
   * Returns all available SDC Log files
   *
   * @return List<Map<String, Object>>
   */
  public List<Map<String, Object>> listLogFiles() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/logs/files".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<List<Map<String, Object>>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns SDC Log File Content
   *
   * @param logName
   * @param attachment
   * @return String
   */
  public String getLogFile(String logName, Boolean attachment) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

     // verify the required parameter 'logName' is set
     if (logName == null) {
        throw new ApiException(400, "Missing the required parameter 'logName' when calling getLogFile");
     }

    // create path and map variables
    String path = "/v1/system/logs/files/{logName}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "logName" + "\\}", apiClient.escapeString(logName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "attachment", attachment));

    final String[] accepts = {
      "text/plain"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<String>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Shutdown SDC
   *
   * @return void
   */
  public void shutdown() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/shutdown".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);
  }

  /**
   * Returns Thread Dump along with stack trace
   *
   * @return List<Map<String, Object>>
   */
  public List<Map<String, Object>> getThreadsDump() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/threads".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<List<Map<String, Object>>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }


  /**
   * Enables DPM by getting application token from DPM
   *
   * @return void
   */
  public void enableDPM(DPMInfoJson dpmInfo) throws ApiException {
    Object postBody = dpmInfo;
    byte[] postBinaryBody = null;

    // verify the required parameter 'dpmInfo' is set
    if (dpmInfo == null) {
      throw new ApiException(400, "Missing the required parameter 'dpmInfo' when calling enableDPM");
    }

    // create path and map variables
    String path = "/v1/system/enableDPM".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);
  }

  /**
   * Disable DPM by getting application token from DPM
   *
   * @return void
   */
  public void disableDPM() throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/system/disableDPM".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);
  }

}
