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

// Controller for Preview Configuration Modal Dialog.
angular
  .module('dataCollectorApp.home')
  .controller('PreviewConfigModalInstanceController', function (
    $scope, $rootScope, $modalInstance, pipelineConfig, pipelineStatus, $timeout, pipelineService, api, pipelineConstant
  ) {
    angular.extend($scope, {
      previewConfig: angular.copy(pipelineConfig.uiInfo.previewConfig),
      refreshCodemirror: false,
      snapshotsInfo: [],

      /**
       * Returns Codemirror Options
       * @param options
       * @returns {*}
       */
      getCodeMirrorOptions: function(options) {
        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), options);
      },

      /**
       * Run Preview Command Handler
       */
      runPreview: function() {
        pipelineConfig.uiInfo.previewConfig = $scope.previewConfig;
        $modalInstance.close();
      },

      /**
       * Cancel and Escape Command Handler
       */
      cancel: function() {
        $modalInstance.dismiss('cancel');
      }

    });

    $timeout(function() {
      $scope.refreshCodemirror = true;
    });

    if (pipelineStatus.executionMode !== pipelineConstant.CLUSTER &&
        pipelineStatus.executionMode !== pipelineConstant.CLUSTER_EMR_BATCH &&
        pipelineStatus.executionMode !== pipelineConstant.CLUSTER_BATCH &&
        pipelineStatus.executionMode !== pipelineConstant.CLUSTER_YARN_STREAMING &&
        pipelineStatus.executionMode !== pipelineConstant.CLUSTER_MESOS_STREAMING) {
      api.pipelineAgent.getSnapshotsInfo().then(
        function(res) {
          if (res && res.data && res.data.length) {
            $scope.snapshotsInfo = res.data;
            $scope.snapshotsInfo = _.chain(res.data)
              .filter(function(snapshotInfo) {
                return !snapshotInfo.inProgress;
              })
              .sortBy('timeStamp')
              .value();
          }
        },
        function(res) {
          $scope.common.errors = [res.data];
        }
      );
    }

  });
