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

/**
 * Controller for Import Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ImportModalInstanceController', function ($scope, $modalInstance, api, pipelineInfo, $translate) {
    var errorMsg = 'Not a valid Pipeline Configuration file.';

    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      uploadFile: {},
      createNewPipeline: (pipelineInfo ? false : true),
      pipelineInfo: pipelineInfo,
      newConfig : {
        name: '',
        description: ''
      },

      /**
       * Import button callback function.
       */
      import: function () {
        var reader = new FileReader();

        if ($scope.createNewPipeline && !$scope.newConfig.name) {
          $translate('home.library.nameRequiredValidation').then(function(translation) {
            $scope.common.errors = [translation];
          });

          return;
        }

        reader.onload = function (loadEvent) {
          try {
            var parsedObj = JSON.parse(loadEvent.target.result),
              jsonConfigObj,
              jsonRulesObj;

            if (parsedObj.pipelineConfig) {
              //It is an config and rules envelope
              jsonConfigObj = parsedObj.pipelineConfig;
              jsonRulesObj = parsedObj.pipelineRules;
            } else {
              jsonConfigObj = parsedObj;
            }

            if (!jsonConfigObj.version) {
              jsonConfigObj.version = 1;
            }

            if (jsonConfigObj.uuid) {
              if (pipelineInfo && !$scope.createNewPipeline) { //If pipeline config already exists
                jsonConfigObj.uuid = pipelineInfo.uuid;
                jsonConfigObj.metadata = pipelineInfo.metadata;
                api.pipelineAgent.savePipelineConfig(pipelineInfo.name, jsonConfigObj).
                  then(function(res) {
                    if (jsonRulesObj && jsonRulesObj.uuid) {
                      api.pipelineAgent.getPipelineRules(pipelineInfo.name).
                        then(function(res) {
                          var rulesObj = res.data;
                          rulesObj.metricsRuleDefinitions = jsonRulesObj.metricsRuleDefinitions;
                          rulesObj.dataRuleDefinitions = jsonRulesObj.dataRuleDefinitions;
                          rulesObj.driftRuleDefinitions = jsonRulesObj.driftRuleDefinitions;
                          rulesObj.emailIds = jsonRulesObj.emailIds;

                          api.pipelineAgent.savePipelineRules(pipelineInfo.name, rulesObj).
                            then(function() {
                              $modalInstance.close();
                            });

                        });

                    } else {
                      $modalInstance.close();
                    }
                  },function(data) {
                    $scope.common.errors = [data];
                  });
              } else { // If no pipeline exist or create pipeline option selected
                var newPipelineObject,
                  name,
                  description;

                if ($scope.createNewPipeline) {
                  name = $scope.newConfig.name;
                  description = $scope.newConfig.description;
                } else {
                  name = jsonConfigObj.info.name;
                  description = jsonConfigObj.info.description;
                }

                api.pipelineAgent.createNewPipelineConfig(name, description)
                  .then(function(res) {
                    newPipelineObject = res.data;
                    newPipelineObject.configuration = jsonConfigObj.configuration;
                    newPipelineObject.errorStage = jsonConfigObj.errorStage;
                    newPipelineObject.statsAggregatorStage = jsonConfigObj.statsAggregatorStage;
                    newPipelineObject.uiInfo = jsonConfigObj.uiInfo;
                    newPipelineObject.stages = jsonConfigObj.stages;
                    newPipelineObject.version = jsonConfigObj.version;
                    newPipelineObject.schemaVersion = jsonConfigObj.schemaVersion;
                    newPipelineObject.metadata = jsonConfigObj.metadata;
                    return api.pipelineAgent.savePipelineConfig(name, newPipelineObject);
                  })
                  .then(function(res) {
                    if (jsonRulesObj && jsonRulesObj.uuid) {
                      api.pipelineAgent.getPipelineRules(name).
                        then(function(res) {
                          var rulesObj = res.data;
                          rulesObj.metricsRuleDefinitions = jsonRulesObj.metricsRuleDefinitions;
                          rulesObj.dataRuleDefinitions = jsonRulesObj.dataRuleDefinitions;
                          rulesObj.driftRuleDefinitions = jsonRulesObj.driftRuleDefinitions;
                          rulesObj.emailIds = jsonRulesObj.emailIds;

                          api.pipelineAgent.savePipelineRules(name, rulesObj).
                            then(function() {
                              $modalInstance.close(newPipelineObject);
                            });

                        });

                    } else {
                      $modalInstance.close(newPipelineObject);
                    }
                  },function(res) {
                    $scope.common.errors = [res.data];

                    //Failed to import pipeline. If new pipeline is created during import revert it back.

                    api.pipelineAgent.deletePipelineConfig(name);
                  });
              }

            } else {
              $scope.$apply(function() {
                $scope.common.errors = [errorMsg];
              });
            }
          } catch(e) {
            $scope.$apply(function() {
              $scope.common.errors = [errorMsg];
            });
          }
        };
        reader.readAsText($scope.uploadFile);
      },

      /**
       * Cancel button callback.
       */
      cancel: function () {
        $modalInstance.dismiss('cancel');
      }
    });






  });
