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
 * Controller for Library Pane Delete Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DeleteModalInstanceController', function (
      $scope, $modalInstance, pipelineInfo, api, tracking
    ) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      pipelineInfo: pipelineInfo,
      isList: _.isArray(pipelineInfo),
      operationInProgress: false,
      yes: function() {
        $scope.operationInProgress = true;
        if ($scope.isList) {
          var pipelineIds = _.pluck(pipelineInfo, 'pipelineId');
          api.pipelineAgent.deletePipelines(pipelineIds)
            .then(function() {
              $modalInstance.close(pipelineInfo);
              tracking.mixpanel.track('Pipelines Bulk Deleted', {'Pipeline IDs': pipelineIds});
            })
            .catch(function(res) {
              $scope.operationInProgress = false;
              $scope.common.errors = [res.data];
            });

        } else {
          api.pipelineAgent.deletePipelineConfig(pipelineInfo.pipelineId)
            .then(function() {
              $modalInstance.close(pipelineInfo);
              tracking.mixpanel.track('Pipeline Deleted', {'Pipeline ID': pipelineInfo.pipelineId});
            })
            .catch(function(res) {
              $scope.operationInProgress = false;
              $scope.common.errors = [res.data];
            });
        }
      },

      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
