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
 * Controller for Connection to server lost Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('ConnectionLostModalInstanceController', function (
    $scope,
    $rootScope,
    $modalInstance,
    $modalStack,
    $timeout,
    api
  ) {
    angular.extend($scope, {
      nextRetryInSeconds: 0,
      retryCountDown: 0,
      common: {
        errors: []
      },
      isRetryingInProgress: false,

      retryNow: function() {
        retryToConnect();
      },

      refreshBrowser: function() {
        window.location.reload();
      }
    });

    var nextRetryInSeconds = 1;
    var maxAttempts = 50;
    var attempts = 0;
    var retryCountDownTimer;
    var updateRetryCountdown = function() {
      $scope.retryCountDown = Math.min(nextRetryInSeconds, 16);

      if(retryCountDownTimer) {
        $timeout.cancel(retryCountDownTimer);
      }

      var retryCountDownCallback = function(){
        $scope.retryCountDown--;
        if($scope.retryCountDown > 0) {
          retryCountDownTimer = $timeout(retryCountDownCallback,1000);
        } else {
          $scope.retryCountDown = 0;
          retryToConnect();
        }
      };
      retryCountDownTimer = $timeout(retryCountDownCallback, 1000);
    };

    var retryToConnect = function() {
      if (attempts > maxAttempts) {
        window.location.reload();
      }
      attempts++;
      nextRetryInSeconds *= 2;

      $scope.isRetryingInProgress = true;
      api.admin.getUserInfo()
        .then(function(res) {
          window.location.reload();
        })
        .catch(function(res) {
          if (res.status === 403 || res.status === 401) {
            window.location.reload();
          } else {
            $scope.isRetryingInProgress = false;
            updateRetryCountdown();
          }
        });
    };

    retryToConnect();
  });
