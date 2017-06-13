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
 * Controller for Log Config Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('LogConfigModalInstanceController', function ($scope, $modalInstance, api, pipelineService, $timeout) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      logConfig: undefined,
      isSaveInProgress: false,
      refreshCodemirror: false,

      save: function() {
        $scope.isSaveInProgress = true;
        api.log.updateLogConfig($scope.logConfig)
          .then(function(res) {
            $modalInstance.close();
          }, function(res) {
            $scope.isSaveInProgress = false;
            $scope.common.errors = [res.data];
          });
      },

      reset: function() {
        api.log.getLogConfig(true)
          .then(function(res) {
            $scope.showLoading = false;
            $scope.logConfig = res.data;

            $scope.refreshCodemirror = true;
            $timeout(function () {
              $scope.refreshCodemirror = false;
            }, 100);
          }, function(res) {
            $scope.showLoading = false;
          });
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      },

      getCodeMirrorOptions: function(options) {
        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), options);
      }
    });

    api.log.getLogConfig()
      .then(function(res) {
        $scope.showLoading = false;
        $scope.logConfig = res.data;

        $scope.refreshCodemirror = true;
        $timeout(function () {
          $scope.refreshCodemirror = false;
        }, 100);

      }, function(res) {
        $scope.showLoading = false;
      });

  });
