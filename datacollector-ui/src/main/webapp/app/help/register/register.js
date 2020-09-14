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
  .controller('RegisterModalInstanceController', function (
      $scope, $rootScope, $modalInstance, $location, $interval, $q,
      api, activationInfo, configuration, authService, showKeyEntry,
      activationTracking, $httpParamSerializer
    ) {

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
          var previousActivationInfo = $scope.activationInfo;
          $scope.activationInfo = res.data;
          if ($scope.activationInfo && $scope.activationInfo.info.valid) {
            $scope.operationDone = true;
            $scope.common.errors = [];
            activationTracking.trackActivationEvent($scope.activationInfo, previousActivationInfo);
          } else {
            $scope.common.errors = ['Uploaded activation key is not valid'];
          }
          $scope.operationInProgress = false;
        },
        function(err) {
          var ERROR_CANNOT_VERIFY = 'java.lang.RuntimeException: java.io.IOException: com.streamsets.datacollector.activation.signed.VerifierException: Could not verify signature';
          var ERROR_INVALID = 'java.lang.RuntimeException: java.io.IOException: com.streamsets.datacollector.activation.signed.VerifierException: Invalid value, cannot verify';
          if (err.data) {
            if (err.data.RemoteException &&
                err.data.RemoteException.message &&
                (err.data.RemoteException.message === ERROR_CANNOT_VERIFY ||
                  err.data.RemoteException.message === ERROR_INVALID)) {
              $scope.common.errors = ['The entered activation code is invalid, verify that it is the same as what you were emailed'];
            } else if (err.status === 403) {
              $scope.common.errors = ['You are not allowed to activate, activation must be done by a user with the Admin role'];
            } else if (typeof err.data === 'string') {
              $scope.common.errors = [err.data];
            } else {
              $scope.common.errors = ['Unable to verify activation code'];
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

    function getInitialActivationStep(activationInfo, showKeyEntry) {
      if (showKeyEntry) {
        return 2;
      } else if (getActivationKeyFromURL()) {
        return 2;
      } else if (activationInfo.info &&
        activationInfo.info.valid &&
        authService.daysUntilProductExpiration(activationInfo.info.expiration) > 0) {
        return 3;
      } else {
        return 1;
      }
    }

    /**
     * Go to activation confirmation page
     */
    function goToConfirmation() {
      $scope.activationStep = 4;
    }

    angular.extend($scope, {
      common: {
        errors: []
      },
      uploadFile: {},
      operationDone: false,
      operationInProgress: false,
      activationInfo: activationInfo,
      showEmailSent: !getActivationKeyFromURL() && !showKeyEntry && activationInfo.info && activationInfo.info.expiration === 0,
      activationStep: getInitialActivationStep(activationInfo, showKeyEntry),
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
        sdcVersion: '',
        agreedToTerms: false,
        agreedToPrivacyPolicy: false
      },
      registerWithAccountUrl: configuration.getAccountRegistrationURL(),

      uploadActivationText: function() {
        uploadActivation($scope.activationData.activationText).then(function(res) {
          goToConfirmation();
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

      goToKeyEntry: function() {
        $scope.showEmailSent = false;
        $scope.activationStep = 2;
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
          window.location.href
          // $location.protocol() + '://' + $location.host() + ':' + $location.port()
        ).then(function(res) {
          $scope.operationInProgress = false;
          $scope.showEmailSent = true;
          $scope.activationStep = 2;
        }, function(err) {
          $scope.operationInProgress = false;
          if (err.data === 'Error: Too many attempts') {
            $scope.common.errors = ['You seem to be having trouble registering, and you will need to wait at least an hour before trying again. ' +
              'If you are a customer, please reach out to support@streamsets.com. Otherwise, community support options can be found at ' +
              'https://streamsets.com/community/'];
          } else {
            $scope.common.errors = ['We had trouble contacting the registration server, please try again'];
          }
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

    if ($scope.activationStep === 1) {
      api.admin.getAsterRegistrationInfo().then(function(response) {
        console.log(response);
        if (response && response.data && response.data.parameters) {
          var val = response.data;
          window.location = val.authorizeUri + '?' + new URLSearchParams(val.parameters);
        }
      });
    }

    $q.all([api.admin.getSdcId(), api.admin.getBuildInfo()]).then( function(results) {
      var sdcId = results[0].data.id;
      $scope.activationData.sdcId = sdcId;
      if (results[1] && results[1].data) {
        var productVersion = results[1].data.version;
        $scope.activationData.sdcVersion = productVersion;
        var accountUrl = configuration.getAccountRegistrationURL();
        if (accountUrl) {
          $scope.registerWithAccountUrl = accountUrl;
        }
      }
      if (getActivationKeyFromURL()) {
        $scope.uploadActivationText();
      }
    });

    // Check if the user was valid due to limited number of stage libraries
    previouslyValid = activationInfo.info.valid;
    if (getInitialActivationStep(activationInfo, showKeyEntry) === 1) {
      activationUpdateInterval = $interval(function() {
        if ($scope.activationStep === 2 && $scope.showEmailSent) {
          api.activation.getActivation().then(function(res) {
            var newActivationInfo = res.data;
            if (previouslyValid) {
              if(authService.daysUntilProductExpiration(newActivationInfo.info.expiration) > 0) {
                $rootScope.common.activationInfo = newActivationInfo;
                $scope.cancel();
              }
            } else {
              if (res.data.info.valid) {
                $scope.closeAndReload();
              }
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
