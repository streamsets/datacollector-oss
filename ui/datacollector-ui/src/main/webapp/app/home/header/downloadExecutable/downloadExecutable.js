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

angular
  .module('dataCollectorApp.home')
  .controller('DownloadEdgeExecutableController', function ($scope, $modalInstance, pipelineConfig, api, pipelineConstant) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      pipelineConfig: pipelineConfig,
      pipelineConstant: pipelineConstant,
      downloadModel: {
        selectedEdgeOs: pipelineConstant.DARWIN_OS,
        selectedEdgeArch: pipelineConstant.AMD64_ARCH
      },
      downloaded: false,

      download: function() {
        api.pipelineAgent.downloadEdgeExecutable(
          $scope.downloadModel.selectedEdgeOs,
          $scope.downloadModel.selectedEdgeArch,
          [pipelineConfig.pipelineId]
        );
        $scope.downloaded = true;
      },

      done: function () {
        $modalInstance.close();
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });

  });
