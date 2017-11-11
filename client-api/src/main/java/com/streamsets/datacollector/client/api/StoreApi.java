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

import com.streamsets.datacollector.client.model.Order;
import com.streamsets.datacollector.client.model.PipelineConfigurationJson;
import com.streamsets.datacollector.client.model.PipelineEnvelopeJson;
import com.streamsets.datacollector.client.model.PipelineInfoJson;
import com.streamsets.datacollector.client.model.PipelineOrderByFields;
import com.streamsets.datacollector.client.model.RuleDefinitionsJson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
   * @param pipelineId
   * @param rev
   * @param get
   * @param attachment
   * @return PipelineConfigurationJson
   */
  public PipelineConfigurationJson getPipelineInfo (String pipelineId, String rev, String get, Boolean attachment)
      throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getPipelineInfo");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

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
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Add a new Pipeline Configuration to the store
   *
   * @param pipelineId
   * @param description
   * @param autoGeneratePipelineId
   * @return PipelineConfigurationJson
   */
  public PipelineConfigurationJson createPipeline (
      String pipelineId,
      String description,
      boolean autoGeneratePipelineId
  ) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling createPipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    queryParams.addAll(apiClient.parameterToPairs("", "description", description));
    queryParams.addAll(apiClient.parameterToPairs("", "autoGeneratePipelineId", autoGeneratePipelineId));

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<PipelineConfigurationJson>() {};
    return apiClient.invokeAPI(path, "PUT", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Add a new Pipeline Configuration to the store
   *
   * @param pipelineId
   * @param description
   * @param autoGeneratePipelineId
   * @return PipelineEnvelopeJson
   */
  public PipelineEnvelopeJson createDraftPipeline (
      String pipelineId,
      String description,
      boolean autoGeneratePipelineId
  ) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling createPipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    queryParams.addAll(apiClient.parameterToPairs("", "description", description));
    queryParams.addAll(apiClient.parameterToPairs("", "autoGeneratePipelineId", autoGeneratePipelineId));
    queryParams.addAll(apiClient.parameterToPairs("", "draft", true));

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    TypeRef returnType = new TypeRef<PipelineEnvelopeJson>() {};
    return apiClient.invokeAPI(path, "PUT", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Update an existing Pipeline Configuration by name
   *
   * @param pipelineId
   * @param pipeline
   * @param rev
   * @param description
   * @return PipelineConfigurationJson
   */
  public PipelineConfigurationJson savePipeline (
      String pipelineId,
      PipelineConfigurationJson pipeline,
      String rev,
      String description
  ) throws ApiException {
    Object postBody = pipeline;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling savePipeline");
    }

    // verify the required parameter 'pipeline' is set
    if (pipeline == null) {
      throw new ApiException(400, "Missing the required parameter 'pipeline' when calling savePipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

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
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Delete Pipeline Configuration by name
   *
   * @param pipelineId
   * @return void
   */
  public void deletePipeline (String pipelineId) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling deletePipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

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

    apiClient.invokeAPI(path, "DELETE", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);
  }

  /**
   * Find Pipeline Rules by name and revision
   *
   * @param pipelineId
   * @param rev
   * @return RuleDefinitionsJson
   */
  public RuleDefinitionsJson getPipelineRules (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getPipelineRules");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/rules".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

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
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Update an existing Pipeline Rules by name
   *
   * @param pipelineId
   * @param pipeline
   * @param rev
   * @return RuleDefinitionsJson
   */
  public RuleDefinitionsJson savePipelineRules (
      String pipelineId,
      RuleDefinitionsJson pipeline,
      String rev
  ) throws ApiException {
    Object postBody = pipeline;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling savePipelineRules");
    }

    // verify the required parameter 'pipeline' is set
    if (pipeline == null) {
      throw new ApiException(400, "Missing the required parameter 'pipeline' when calling savePipelineRules");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/rules".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

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
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns all Pipeline Configuration Info
   *
   * @return List<PipelineInfoJson>
   */
  public List<PipelineInfoJson> getPipelines (
      String filterText,
      String label,
      int offset,
      int len,
      PipelineOrderByFields orderBy,
      Order order,
      boolean includeStatus
  ) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/pipelines".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    queryParams.addAll(apiClient.parameterToPairs("", "filterText", filterText));
    queryParams.addAll(apiClient.parameterToPairs("", "label", label));
    queryParams.addAll(apiClient.parameterToPairs("", "offset", offset));
    queryParams.addAll(apiClient.parameterToPairs("", "len", len));
    queryParams.addAll(apiClient.parameterToPairs("", "orderBy", orderBy));
    queryParams.addAll(apiClient.parameterToPairs("", "order", order));

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };
    TypeRef returnType = new TypeRef<List<PipelineInfoJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }


  /**
   * Export Pipeline Configuration & Rules by name and revision
   *
   * @param pipelineId
   * @param rev
   * @param attachment
   * @param includeLibraryDefinitions
   * @return PipelineEnvelopeJson
   */
  public PipelineEnvelopeJson exportPipeline (
      String pipelineId,
      String rev,
      Boolean attachment,
      Boolean includeLibraryDefinitions
  ) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getPipelineInfo");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/export"
        .replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));
    queryParams.addAll(apiClient.parameterToPairs("", "attachment", attachment));
    queryParams.addAll(apiClient.parameterToPairs("", "includeLibraryDefinitions", includeLibraryDefinitions));

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };


    TypeRef returnType = new TypeRef<PipelineEnvelopeJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Import Pipeline Configuration & Rules
   *
   * @param pipelineId
   * @param rev
   * @param overwrite
   * @param autoGeneratePipelineId
   * @return PipelineEnvelopeJson
   */
  public PipelineEnvelopeJson importPipeline (
      String pipelineId,
      String rev,
      Boolean overwrite,
      Boolean autoGeneratePipelineId,
      boolean draft,
      PipelineEnvelopeJson pipelineEnvelope
  ) throws ApiException {
    Object postBody = pipelineEnvelope;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling importPipeline");
    }

    // verify the required parameter 'pipeline' is set
    if (pipelineEnvelope == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineEnvelope' when calling importPipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/import".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));
    queryParams.addAll(apiClient.parameterToPairs("", "overwrite", overwrite));
    queryParams.addAll(apiClient.parameterToPairs("", "autoGeneratePipelineId", autoGeneratePipelineId));
    queryParams.addAll(apiClient.parameterToPairs("", "draft", draft));

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {
        "application/json"
    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };


    TypeRef returnType = new TypeRef<PipelineEnvelopeJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Deletes filtered Pipelines
   *
   * @return List<String>
   */
  public List<String> deletePipelinesByFiltering (String filterText, String label) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/pipelines/deleteByFiltering".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();

    queryParams.addAll(apiClient.parameterToPairs("", "filterText", filterText));
    queryParams.addAll(apiClient.parameterToPairs("", "label", label));

    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };
    TypeRef returnType = new TypeRef<List<String>>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }
}
