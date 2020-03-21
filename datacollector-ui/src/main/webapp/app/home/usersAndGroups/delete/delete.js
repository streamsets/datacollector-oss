/*
 * Copyright 2020 StreamSets Inc.
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
 * Controller for Delete User Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DeleteModalController', function ($scope, $modalInstance, userId, isLastAdmin, api) {
    angular.extend($scope, {
      userId: userId,
      isLastAdmin: isLastAdmin,
      operationInProgress: false,

      yes: function() {
        $scope.operationInProgress = true;
        api.admin.deleteUser(userId)
          .then(function() {
            $modalInstance.close(userId);
          })
          .catch(function(res) {
            $scope.operationInProgress = false;
            var errorMessages = ['Error'];
            if(res.data) {
              if(res.data.type==='ERROR') {
                errorMessages = res.data.messages.map(function(obj) {return obj.message;});
              } else {
                errorMessages = [res.data];
              }
            } else {
              errorMessages = [res.status + ' - ' + res.statusText];
            }
            $scope.common.errors = errorMessages;
          });
      },

      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
