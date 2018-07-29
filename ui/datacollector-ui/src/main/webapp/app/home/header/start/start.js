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
 * Controller for Start with parameters.
 */

angular
  .module('dataCollectorApp.home')
  .controller('StartModalInstanceController', function ($scope, $modalInstance, pipelineConfig, api) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      pipelineConfig: pipelineConfig,
      parameters: {
        runtimeParameters: {}
      },
      constantsConfig: undefined,
      starting: false,

      start: function() {
        $scope.starting = true;
        api.pipelineAgent.startPipeline(pipelineConfig.info.pipelineId, 0, $scope.parameters.runtimeParameters)
          .then(
            function(res) {
              $modalInstance.close(res);
            },
            function(res) {
              $scope.starting = false;
              $scope.common.errors = [res.data];
            }
          );
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.runtimeParameters = {};
    var constantsConfig = _.find(pipelineConfig.configuration, function (config) {
      return config.name === 'constants';
    });

    if (constantsConfig) {
      $scope.constantsConfig = constantsConfig;
      angular.forEach(constantsConfig.value, function (constant) {
        $scope.parameters.runtimeParameters[constant.key] = constant.value;
      });
    }

  });
