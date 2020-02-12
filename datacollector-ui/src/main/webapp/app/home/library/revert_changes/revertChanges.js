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
/**
 * Controller for Library Pane Revert Changes Confirmation modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RevertChangesModalInstanceController', function (
    $scope, $modalInstance, pipelineInfo, metadata, api, authService
  ) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      pipelineInfo: pipelineInfo,
      operationInProgress: false,
      dpmPipelineId: metadata['dpm.pipeline.id'],
      dpmPipelineVersion: metadata['dpm.pipeline.version'],
      yes: function() {
        $scope.operationInProgress = true;

        api.controlHub.getPipelineCommitHistory($scope.dpmPipelineId).then(
          function(res) {
            var commits = res.data;
            var remotePipeline;
            if (commits && commits.length) {
              remotePipeline = _.find(commits, function(commit) {
                return commit.version === $scope.dpmPipelineVersion;
              });
            }

            if (remotePipeline) {
              api.controlHub.getPipeline(remotePipeline)
                .then(
                  function(res) {
                    var orgRemotePipeline = res.data;
                    var pipelineEnvelope = {
                      pipelineConfig: JSON.parse(orgRemotePipeline.pipelineDefinition),
                      pipelineRules: JSON.parse(orgRemotePipeline.currentRules.rulesDefinition)
                    };

                    api.pipelineAgent.importPipelineConfig(pipelineInfo.pipelineId, pipelineEnvelope, true, false)
                      .then(
                        function(res) {
                          $scope.updatedPipelineConfig = res.data.pipelineConfig;
                          $scope.pipelineVersion = orgRemotePipeline.version;
                          var newMetadata = res.data.pipelineConfig.metadata;
                          newMetadata['lastConfigId'] = res.data.pipelineConfig.uuid;
                          newMetadata['lastRulesId'] = res.data.pipelineRules.uuid;
                          api.pipelineAgent.savePipelineMetadata(pipelineInfo.pipelineId, newMetadata)
                            .then(
                              function(res) {
                                $modalInstance.close($scope.updatedPipelineConfig);
                              },
                              function(res) {
                                $scope.common.errors = [res.data];
                                $scope.operationInProgress = false;
                              }
                            );
                        },
                        function(res) {
                          $scope.common.errors = [res.data];
                          $scope.operationInProgress = false;
                        }
                      );
                  },
                  function(res) {
                    $scope.common.errors = [res.data];
                  }
                );
            } else {
              $scope.common.errors = ['No pipeline found in Control Hub Pipeline Repository with pipeline ID: ' +
              $scope.dpmPipelineId];
              $scope.operationInProgress = false;
            }
          },
          function(res) {
            $scope.operationInProgress = false;
            $scope.common.errors = [res.data];
          }
        );
      },

      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });


  });
