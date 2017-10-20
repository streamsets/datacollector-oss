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
 * Controller for Library Pane Create Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CreateModalInstanceController', function ($scope, $modalInstance, $translate, api) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      selectedSource: '',
      selectedProcessors: {},
      selectedTargets: {},
      newConfig : {
        name: '',
        description: '',
        executionMode: 'STANDALONE'
      },

      save : function () {
        if($scope.newConfig.name) {
          api.pipelineAgent.createNewPipelineConfig($scope.newConfig.name, $scope.newConfig.description).
            then(
              function(res) {
                if ($scope.newConfig.executionMode === 'STANDALONE') {
                  $modalInstance.close(res.data);
                } else {
                  var newPipelineObject = res.data;

                  // Update execution mode
                  var executionModeConfig = _.find(newPipelineObject.configuration, function(c) {
                    return c.name === 'executionMode';
                  });

                  executionModeConfig.value = $scope.newConfig.executionMode;
                  api.pipelineAgent.savePipelineConfig(newPipelineObject.pipelineId, newPipelineObject)
                    .then(
                      function(res) {
                        $modalInstance.close(res.data);
                      },
                      function(res) {
                        $scope.common.errors = [res.data];
                      }
                    );
                }
              },
              function(res) {
                $scope.common.errors = [res.data];
              }
            );
        } else {
          $translate('home.library.nameRequiredValidation').then(function(translation) {
            $scope.common.errors = [translation];
          });

        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

  });
