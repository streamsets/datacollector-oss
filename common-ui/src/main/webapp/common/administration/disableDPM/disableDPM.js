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
 * Controller for Disable SCH Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('DisableDPMModalInstanceController', function ($scope, $modalInstance, api, authService) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      isRemoteUserOrgAdmin: authService.isRemoteUserOrgAdmin(),
      isDisableInProgress: false,
      dpmDisabled: false,
      isRestartInProgress: false,

      onDisableDPMSubmit: function() {
        $scope.common.errors = [];
        $scope.isDisableInProgress = true;
        api.admin.disableDPM()
          .then(function() {
            $scope.isDisableInProgress = false;
            $scope.dpmDisabled = true;
          })
          .catch(function(res) {
            $scope.isDisableInProgress = false;
            $scope.common.errors = [res.data];
          });
      },

      restart: function() {
        $scope.isRestartInProgress = true;
        api.admin.restartDataCollector();
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
