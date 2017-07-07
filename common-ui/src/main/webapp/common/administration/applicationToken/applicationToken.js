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
 * Controller for Application Token Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('ApplicationTokenModalInstanceController', function ($scope, $modalInstance, api, authService) {
    angular.extend($scope, {
      issues: [],
      isGeneratingToken: false,
      isGeneratingTokenSucceed: false,
      isRemoteUserOrgAdmin: authService.isRemoteUserOrgAdmin(),
      isRestartInProgress: false,

      restart: function() {
        $scope.isRestartInProgress = true;
        api.admin.restartDataCollector();
      },

      generateToken: function() {
        $scope.isGeneratingToken = true;
        api.remote.generateApplicationToken(
          authService.getRemoteBaseUrl(),
          authService.getSSOToken(),
          authService.getRemoteOrgId()
        ).then(function(response) {
          var res = response.data;
          var authToken = res[0].fullAuthToken;
          api.admin.updateApplicationToken(authToken)
            .then(function() {
              $scope.isGeneratingTokenSucceed = true;
              $scope.isGeneratingToken = false;
            })
            .catch(function(res) {
              $scope.issues = [res.data];
              $scope.isGeneratingToken = false;
            });
        }).catch(function(res) {
          $scope.issues = [res.data];
          $scope.isGeneratingToken = false;
        });

      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
