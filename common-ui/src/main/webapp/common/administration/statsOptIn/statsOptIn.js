/*
 * Copyright 2018 StreamSets Inc.
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

// Controller for Usage Statistics Modal Dialog.
angular
  .module('dataCollectorApp')
  .controller('StatsOptInController', function ($scope, $modalInstance,  api) {
    angular.extend($scope, {
      isLoading: true,
      currentStatus: undefined,

      cancel: function() {
        $modalInstance.close();
      },

      save: function() {
        api.system.setOptInStatus($scope.currentStatus.active)
          .then(function() {
            $modalInstance.close();
          });
      }
    });

    api.system.getStats()
      .then(function(res) {
        console.log(res);
        $scope.currentStatus = res.data;
        $scope.isLoading = false;
      });
  });
