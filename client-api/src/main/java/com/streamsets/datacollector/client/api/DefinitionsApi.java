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

import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.Configuration;
import com.streamsets.datacollector.client.Pair;
import com.streamsets.datacollector.client.TypeRef;
import com.streamsets.pipeline.api.HideStage;

import com.streamsets.datacollector.client.model.DefinitionsJson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.HashMap;

public class DefinitionsApi {
  private ApiClient apiClient;

  public DefinitionsApi() {
    this(Configuration.getDefaultApiClient());
  }

  public DefinitionsApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  /**
   * Returns pipeline &amp; stage configuration definitions
   * This is to fetch all the definitions
   *
   * @return DefinitionsJson
   */
  @Deprecated
  public DefinitionsJson getDefinitions () throws ApiException {
    return this.getDefinitions(null);
  }

  /**
   * Returns pipeline &amp; stage configuration definitions
   * This will fetch defintions based on the hideStage filter
   *
   * @return DefinitionsJson
   */
  public DefinitionsJson getDefinitions (HideStage.Type hideStage) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/definitions";

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    if (hideStage != null) {
      queryParams.add(new Pair("hideStage", hideStage.name()));
    }
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

    TypeRef returnType = new TypeRef<DefinitionsJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams,
        formParams, accept, contentType, authNames, returnType);
  }

  /**
   * Returns HELP Reference
   *
   * @return Map<String, Object>
   */
  public Map<String, Object> getHelpRefs () throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/definitions/helpref".replaceAll("\\{format\\}","json");

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
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams,
        formParams, accept, contentType, authNames, returnType);
  }

  /**
   * Return stage icon for library and stage name
   *
   * @param library
   * @param stageName
   * @return Object
   */
  public Object getIcon (String library, String stageName) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'library' is set
    if (library == null) {
      throw new ApiException(400, "Missing the required parameter 'library' when calling getIcon");
    }

    // verify the required parameter 'stageName' is set
    if (stageName == null) {
      throw new ApiException(400, "Missing the required parameter 'stageName' when calling getIcon");
    }

    // create path and map variables
    String path = "/v1/definitions/stages/{library}/{stageName}/icon".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "library" + "\\}", apiClient.escapeString(library.toString()))
      .replaceAll("\\{" + "stageName" + "\\}", apiClient.escapeString(stageName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    final String[] accepts = {
      "image/svg+xml", "image/png"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<Object>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams,
        accept, contentType, authNames, returnType);
  }

}
