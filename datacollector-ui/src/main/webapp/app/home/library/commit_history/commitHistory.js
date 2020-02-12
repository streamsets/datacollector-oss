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
 * Controller for Remote Pipeline Commit History Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CommitHistoryModalInstanceController', function (
    $scope, $modalInstance, api, authService, pipelineInfo, metadata
  ) {
    angular.extend($scope, {
      remoteBaseUrl: authService.getRemoteBaseUrl(),
      common: {
        errors: []
      },
      pipelineInfo: pipelineInfo,
      pipelineId: metadata['dpm.pipeline.id'],
      pipelineVersion: metadata['dpm.pipeline.version'],
      pipelinesCommit: [],
      updatedPipelineConfig: undefined,

      downloadRemotePipeline : function(remotePipeline) {
        $scope.downloading = true;
        api.controlHub.getPipeline(remotePipeline)
          .then(
            function(res) {
              var remotePipeline = res.data;
              var pipelineEnvelope = {
                pipelineConfig: JSON.parse(remotePipeline.pipelineDefinition),
                pipelineRules: JSON.parse(remotePipeline.currentRules.rulesDefinition)
              };

              api.pipelineAgent.importPipelineConfig(pipelineInfo.pipelineId, pipelineEnvelope, true)
                .then(
                  function(res) {
                    $scope.updatedPipelineConfig = res.data.pipelineConfig;
                    $scope.pipelineVersion = remotePipeline.version;
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
                          $scope.downloading = false;
                        }
                      );
                  },
                  function(res) {
                    $scope.common.errors = [res.data];
                    $scope.downloading = false;
                  }
                );
            },
            function(res) {
              $scope.common.errors = [res.data];
            }
          );
      },
      close : function () {
        $modalInstance.close();
      }
    });

    var getPipelineCommitHistory = function() {
      api.controlHub.getPipelineCommitHistory($scope.pipelineId)
        .then(
          function(res) {
            $scope.pipelinesCommit = res.data;
          },
          function(res) {
            $scope.common.errors = [res.data];
          }
        );
    };

    getPipelineCommitHistory();

  });
