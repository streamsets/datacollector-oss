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
 * Controller for Add Label Confirmation Modal.
 */

angular
    .module('dataCollectorApp.home')
    .controller('AddLabelConfirmationModalInstanceController', function ($scope, $modalInstance, pipelineInfoList, api) {
      angular.extend($scope, {
        pipelineInfoList: pipelineInfoList,
        data: {
          labels: []
        },
        common: {
          errors: []
        },

        save: function() {
          var pipelineNames = _.pluck(pipelineInfoList, 'name');
          api.pipelineAgent.addLabelsToPipelines($scope.data.labels, pipelineNames)
            .then(function(response) {
              var res = response.data;
              if (res.errorMessages.length === 0) {
                $modalInstance.close(_.extend(res, {labels: $scope.data.labels}));
              } else {
                $scope.common.errors = res.errorMessages;
              }
            })
            .catch(function(res) {
              $scope.common.errors = [res.data];
            });
        },
        cancel: function() {
          $modalInstance.dismiss('cancel');
        }
      });
    });
