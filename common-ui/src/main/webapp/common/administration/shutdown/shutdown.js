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
 * Controller for Shutdown Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('ShutdownModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      issues: [],
      isShuttingDown: false,
      isShutdownSucceed: false,

      shutdown: function() {
        $scope.isShuttingDown = true;
        api.admin.shutdownCollector()
          .then(function() {
            $scope.isShutdownSucceed = true;
            $scope.isShuttingDown = false;
          })
          .catch(function(res) {
            $scope.issues = [res.data];
            $scope.isShuttingDown = false;
          });
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
