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
package com.streamsets.datacollector.stagelibrary;

import com.streamsets.datacollector.classpath.ClasspathValidatorResult;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.InterceptorDefinition;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineFragmentDefinition;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageLibraryDelegateDefinitition;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinition;
import com.streamsets.datacollector.restapi.bean.EventDefinitionJson;
import com.streamsets.datacollector.restapi.bean.RepositoryManifestJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionMinimalJson;
import com.streamsets.datacollector.task.Task;
import com.streamsets.pipeline.api.impl.annotationsprocessor.PipelineAnnotationsProcessor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface StageLibraryTask extends Task, ClassLoaderReleaser {

  String STAGES_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.STAGES_FILE;

  String EL_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.ELDEFS_FILE;

  String LINEAGE_PUBLISHERS_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.LINEAGE_PUBLISHERS_FILE;

  String CREDENTIAL_STORE_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.CREDENTIAL_STORE_FILE;

  String SERVICE_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.SERVICES_FILE;

  String INTERCEPTOR_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.INTERCEPTORS_FILE;

  String DELEGATE_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.DELEGATE_LIST_FILE;

  String CONNECTIONS_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.CONNECTIONS_LIST_FILE;

  String CONNECTION_VERIFIERS_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.CONNECTION_VERIFIERS_LIST_FILE;

  PipelineDefinition getPipeline();

  PipelineFragmentDefinition getPipelineFragment();

  PipelineRulesDefinition getPipelineRules();

  List<StageDefinition> getStages();

  List<LineagePublisherDefinition> getLineagePublisherDefinitions();

  LineagePublisherDefinition getLineagePublisherDefinition(String library, String name);

  List<CredentialStoreDefinition> getCredentialStoreDefinitions();

  List<ServiceDefinition> getServiceDefinitions();

  ServiceDefinition getServiceDefinition(Class serviceInterface, boolean forExecution);

  List<InterceptorDefinition> getInterceptorDefinitions();

  StageDefinition getStage(String library, String name, boolean forExecution);

  Map<String, String> getLibraryNameAliases();

  Map<String, String> getStageNameAliases();

  List<ClasspathValidatorResult> validateStageLibClasspath();

  List<StageLibraryDelegateDefinitition> getStageLibraryDelegateDefinitions();

  StageLibraryDelegateDefinitition getStageLibraryDelegateDefinition(String  stageLibrary, Class exportedInterface);

  List<StageLibraryDefinition> getLoadedStageLibraries();

  List<RepositoryManifestJson> getRepositoryManifestList();

  boolean isMultipleOriginSupported();

  List<String> getLegacyStageLibs();

  Map<String, EventDefinitionJson> getEventDefinitions();

  StageLibraryDefinition getStageLibraryDefinition(String libraryName);

  Collection<ConnectionDefinition> getConnections();

  ConnectionDefinition getConnection(String type);

  Set<ConnectionVerifierDefinition> getConnectionVerifiers(String type);

  List<StageDefinitionMinimalJson> getStageDefinitionMinimalList();
}
