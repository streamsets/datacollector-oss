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
  .controller('RegisterModalInstanceController', function ($scope, $rootScope, $modalInstance, $location, $interval,
    api, activationInfo, configuration, authService) {
    
    var activationUpdateInterval;
    var previouslyValid = false;

    /**
     * Upload the activation key
     * @param {String} keyText
     */
    function uploadActivation(keyText) {
      $scope.operationInProgress = true;
      return api.activation.updateActivation(keyText)
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
        function(err) {
          if (err.data) {
            if (err.data.RemoteException && err.data.RemoteException.message &&
              err.data.RemoteException.message === 'java.lang.RuntimeException: java.io.IOException: com.streamsets.datacollector.activation.signed.VerifierException: Could not verify signature') {
              $scope.common.errors = ['The entered activation code is invalid'];
            } else {
              $scope.common.errors = [err.data];
            }
          } else {
            $scope.common.errors = ['Unable to verify activation code'];
          }
          
          $scope.operationDone = false;
          $scope.operationInProgress = false;
          throw err;
        }
      );
    }

    function getActivationKeyFromURL() {
      return $location.search().activationKey;
    }

    function getInitialActivationStep(activationInfo) {
      if (getActivationKeyFromURL()) {
        return 2;
      } else if (activationInfo.info && 
        activationInfo.info.valid && 
        authService.daysUntilProductExpiration(activationInfo.info.expiration) > 0) {
        return 3;
      } else {
        return 1;
      }
    }

    angular.extend($scope, {
      common: {
        errors: []
      },
      uploadFile: {},
      operationDone: false,
      operationInProgress: false,
      activationInfo: activationInfo,
      activationKeyFilledFromURL: Boolean(getActivationKeyFromURL()),
      activationStep: getInitialActivationStep(activationInfo),
      activationData: {
        activationText: '',
        firstName: '',
        lastName: '',
        companyName: '',
        email: '',
        role: '',
        country: '',
        postalCode: '',
        sdcId: '',
        sdcVersion: ''
      },

      uploadActivationText: function() {
        uploadActivation($scope.activationData.activationText).then(function(res) {
          $modalInstance.dismiss();
          window.location.reload();
        });
      },

      /**
       * Upload button callback function.
       */
      uploadActivationKey: function () {
        $scope.operationInProgress = true;
        var reader = new FileReader();
        reader.onload = function (loadEvent) {
          try {
            var parsedObj = loadEvent.target.result;
            uploadActivation(parsedObj);
          } catch(e) {
            $scope.$apply(function() {
              $scope.common.errors = [e];
            });
          }
        };
        reader.readAsText($scope.uploadFile);
      },

      goToRegistration: function() {
        $scope.activationStep = 1;
      },

      sendRegistration: function() {
        $scope.operationInProgress = true;
        api.externalRegistration.sendRegistration(
          configuration.getRegistrationURL(),
          $scope.activationData.firstName,
          $scope.activationData.lastName,
          $scope.activationData.companyName,
          $scope.activationData.email,
          $scope.activationData.role,
          $scope.activationData.country,
          $scope.activationData.postalCode,
          $scope.activationData.sdcId,
          $scope.activationData.sdcVersion,
          $location.protocol() + '://' + $location.host() + ':' + $location.port()
        ).then(function(res) {
          $scope.operationInProgress = false;
          $scope.activationStep = 2;
        }, function(err) {
          $scope.operationInProgress = false;
          $scope.common.errors = ['We had trouble contacting the registration server, please try again'];
        });
      },

      /**
       * Cancel button callback.
       */
      cancel: function () {
        $modalInstance.dismiss('cancel');
      },

      /**
       * Close button callback, after new activation file uploaded
       */
      closeAndReload: function () {
        $modalInstance.dismiss('cancel');
        window.location.reload();
      }
    });

    if (getActivationKeyFromURL()) {
      $scope.activationStep = 2;
      $scope.activationData.activationText = decodeURI(getActivationKeyFromURL());
    }

    api.admin.getSdcId().then(function(res) {
      $scope.activationData.sdcId = res.data.id;
    });
    api.admin.getBuildInfo().then(function(res) {
      if (res && res.data) {
        $scope.activationData.sdcVersion = res.data.version;
      }
    });

    // Check if the user was valid due to limited number of stage libraries
    previouslyValid = $scope.activationInfo.info.valid;
    if (getInitialActivationStep($scope.activationInfo) === 1 && previouslyValid) {
      activationUpdateInterval = $interval(function() {
        if ($scope.activationStep === 2) {
          api.activation.getActivation().then(function(res) {
            var activationInfo = res.data;
            if(authService.daysUntilProductExpiration(activationInfo.info.expiration) > 0) {
              $rootScope.common.activationInfo = activationInfo;
              $scope.cancel();
            }
          });
        }
      }, 2000);      
    }

    $scope.$on('$destroy', function() {
      if (angular.isDefined(activationUpdateInterval)) {
        $interval.cancel(activationUpdateInterval);
      }
    });
  });
