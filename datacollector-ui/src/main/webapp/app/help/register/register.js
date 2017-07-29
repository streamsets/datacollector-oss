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
 * Controller for Register Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('RegisterModalInstanceController', function ($scope, $modalInstance, api, activationInfo) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      uploadFile: {},
      operationDone: false,
      operationInProgress: false,
      activationInfo: activationInfo,

      /**
       * Upload button callback function.
       */
      uploadActivationKey: function () {
        $scope.operationInProgress = true;
        var reader = new FileReader();
        reader.onload = function (loadEvent) {
          try {
            var parsedObj = loadEvent.target.result;
            api.activation.updateActivation(parsedObj)
              .then(
                function(res) {
                  $scope.activationInfo = res.data;
                  if ($scope.activationInfo && $scope.activationInfo.info.valid) {
                    $scope.operationDone = true;
                    $scope.common.errors = [];
                  } else {
                    $scope.common.errors = ['Uploaded activation key is not valid'];
                  }
                  $scope.operationInProgress = false;
                },
                function(res) {
                  $scope.common.errors = [res.data];
                  $scope.operationDone = false;
                  $scope.operationInProgress = false;
                }
              );
          } catch(e) {
            $scope.$apply(function() {
              $scope.common.errors = [e];
            });
          }
        };
        reader.readAsText($scope.uploadFile);
      },

      /**
       * Cancel button callback.
       */
      cancel: function () {
        $modalInstance.dismiss('cancel');
        window.location.reload();
      }
    });


  });
