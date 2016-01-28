/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.datacollector.client.model.*;

import java.util.*;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2015-09-11T14:51:29.367-07:00")
public class StoreApi {
  private ApiClient apiClient;

  public StoreApi() {
    this(Configuration.getDefaultApiClient());
  }

  public StoreApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }


  /**
   * Find Pipeline Configuration by name and revision
   *
   * @param pipelineName
   * @param rev
   * @param get
   * @param attachment
   * @return PipelineConfigurationJson
   */
  public PipelineConfigurationJson getPipelineInfo (String pipelineName, String rev, String get, Boolean attachment) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

     // verify the required parameter 'pipelineName' is set
     if (pipelineName == null) {
        throw new ApiException(400, "Missing the required parameter 'pipelineName' when calling getPipelineInfo");
     }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineName}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineName" + "\\}", apiClient.escapeString(pipelineName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "get", get));

    queryParams.addAll(apiClient.parameterToPairs("", "attachment", attachment));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PipelineConfigurationJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Add a new Pipeline Configuration to the store
   *
   * @param pipelineName
   * @param description
   * @return PipelineConfigurationJson
   */
  public PipelineConfigurationJson createPipeline (String pipelineName, String description) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

     // verify the required parameter 'pipelineName' is set
     if (pipelineName == null) {
        throw new ApiException(400, "Missing the required parameter 'pipelineName' when calling createPipeline");
     }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineName}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineName" + "\\}", apiClient.escapeString(pipelineName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "description", description));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PipelineConfigurationJson>() {};
    return apiClient.invokeAPI(path, "PUT", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Update an existing Pipeline Configuration by name
   *
   * @param pipelineName
   * @param pipeline
   * @param rev
   * @param description
   * @return PipelineConfigurationJson
   */
  public PipelineConfigurationJson savePipeline (String pipelineName, PipelineConfigurationJson pipeline, String rev, String description) throws ApiException {
    Object postBody = pipeline;
    byte[] postBinaryBody = null;

     // verify the required parameter 'pipelineName' is set
     if (pipelineName == null) {
        throw new ApiException(400, "Missing the required parameter 'pipelineName' when calling savePipeline");
     }

     // verify the required parameter 'pipeline' is set
     if (pipeline == null) {
        throw new ApiException(400, "Missing the required parameter 'pipeline' when calling savePipeline");
     }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineName}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineName" + "\\}", apiClient.escapeString(pipelineName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "description", description));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {
      "application/json"
    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PipelineConfigurationJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Delete Pipeline Configuration by name
   *
   * @param pipelineName
   * @return void
   */
  public void deletePipeline (String pipelineName) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

     // verify the required parameter 'pipelineName' is set
     if (pipelineName == null) {
        throw new ApiException(400, "Missing the required parameter 'pipelineName' when calling deletePipeline");
     }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineName}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineName" + "\\}", apiClient.escapeString(pipelineName.toString()));

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





    apiClient.invokeAPI(path, "DELETE", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, null);




  }

  /**
   * Find Pipeline Rules by name and revision
   *
   * @param pipelineName
   * @param rev
   * @return RuleDefinitionsJson
   */
  public RuleDefinitionsJson getPipelineRules (String pipelineName, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

     // verify the required parameter 'pipelineName' is set
     if (pipelineName == null) {
        throw new ApiException(400, "Missing the required parameter 'pipelineName' when calling getPipelineRules");
     }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineName}/rules".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineName" + "\\}", apiClient.escapeString(pipelineName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<RuleDefinitionsJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Update an existing Pipeline Rules by name
   *
   * @param pipelineName
   * @param pipeline
   * @param rev
   * @return RuleDefinitionsJson
   */
  public RuleDefinitionsJson savePipelineRules (String pipelineName, RuleDefinitionsJson pipeline, String rev) throws ApiException {
    Object postBody = pipeline;
    byte[] postBinaryBody = null;

     // verify the required parameter 'pipelineName' is set
     if (pipelineName == null) {
        throw new ApiException(400, "Missing the required parameter 'pipelineName' when calling savePipelineRules");
     }

     // verify the required parameter 'pipeline' is set
     if (pipeline == null) {
        throw new ApiException(400, "Missing the required parameter 'pipeline' when calling savePipelineRules");
     }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineName}/rules".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineName" + "\\}", apiClient.escapeString(pipelineName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<RuleDefinitionsJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Returns all Pipeline Configuration Info
   *
   * @return List<PipelineInfoJson>
   */
  public List<PipelineInfoJson> getPipelines () throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/pipelines".replaceAll("\\{format\\}","json");

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





    TypeRef returnType = new TypeRef<List<PipelineInfoJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

}
