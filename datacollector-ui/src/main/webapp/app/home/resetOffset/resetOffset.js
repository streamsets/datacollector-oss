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
  .controller('ResetOffsetModalInstanceController', function ($scope, $modalInstance, pipelineInfo, originStageDef,
                                                              $translate, api) {
    angular.extend($scope, {
      showLoading: false,
      isOffsetResetSucceed: false,
      common: {
        errors: []
      },
      pipelineInfo: pipelineInfo,
      isList: _.isArray(pipelineInfo),

      /**
       * Callback function Yes button
       */
      yes: function() {
        if ($scope.isList) {
          $scope.showLoading = true;
          api.pipelineAgent.resetOffsets(_.pluck(pipelineInfo, 'pipelineId'))
            .then(function() {
              $scope.showLoading = false;
              $scope.isOffsetResetSucceed = true;
            })
            .catch(function(res) {
              $scope.showLoading = false;
              $scope.common.errors = [res.data];
            });

        } else {
          if(originStageDef.resetOffset) {
            $scope.showLoading = true;
            api.pipelineAgent.resetOffset(pipelineInfo.pipelineId)
              .then(function() {
                $scope.showLoading = false;
                $scope.isOffsetResetSucceed = true;
              })
              .catch(function(res) {
                $scope.showLoading = false;
                $scope.common.errors = [res.data];
              });
          } else {
            $translate('home.resetOffset.noSupport', { label: originStageDef.label })
                .then(function(translation) {
                  $scope.common.errors = [translation];
                });
          }
        }
      },

      /**
       * Callback function for No button
       */
      no: function() {
        $modalInstance.dismiss('cancel');
      },

      /**
       * Callback function for Close button
       */
      close: function() {
        $modalInstance.close(pipelineInfo);
      }
    });
  });
