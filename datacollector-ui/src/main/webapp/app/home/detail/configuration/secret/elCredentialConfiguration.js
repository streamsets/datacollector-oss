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

// Controller for entering credentials
angular
  .module('dataCollectorApp.home')
  .controller('elCredentialConfigurationController', function (
    $scope, api, secretUtil
  ) {
    angular.extend($scope, {
      credentialSettings: {
        credentialValue: '',
        showCredential: false,
        showExpression: false,
        saved: false,
        useInternalSecrets: true,
        showValue: false,
        store: 'streamsets'
      },

      // Save a typed in credential to the interal secret store
      saveCredential: function() {
        var credentialValue = $scope.credentialSettings.credentialValue;
        if (credentialValue.length < 1) {
          return;
        }
        var vaultSecretExpression = secretUtil.getStandardExpression(
          $scope.pipelineConfig.pipelineId,
          $scope.selectedObject.instanceName,
          $scope.configDefinition.name,
          $scope.credentialSettings.store
        );
        var vaultName = vaultSecretExpression.vaultName;
        var secretName = vaultSecretExpression.secretName;
        var expression = vaultSecretExpression.expression;
        api.secret.createOrUpdateTextSecret(
          vaultName,
          secretName,
          credentialValue)
        .then(function(response) {
          if (response.status >=200 && response.status < 300) {
            $scope.detailPaneConfig.configuration[$scope.configIndex].value = expression;
            $scope.credentialSettings.credentialValue = '';
            $scope.credentialSettings.saved = true;
          } else {
            console.error('Could not save credential', response);
          }
        }).catch(function (err) {
          console.error(err);
        });
      },

      expressionValue: function(newValue) {
        if (arguments.length > 0) {
          // setter
          return $scope.detailPaneConfig.configuration[$scope.configIndex].value = newValue;
        } else {
          // getter
          var currentValue = $scope.detailPaneConfig.configuration[$scope.configIndex].value;
          // Do not return value if using internal credential store
          if (!currentValue || secretUtil.isInternalSecret(currentValue, $scope.credentialSettings.store)) {
            return '';
          } else {
            return $scope.detailPaneConfig.configuration[$scope.configIndex].value;
          }
        }
      },

      toggleShowExpression: function() {
        $scope.credentialSettings.showExpression = !$scope.credentialSettings.showExpression;
        $scope.credentialSettings.saved = false;
      },

      toggleShowCredential: function() {
        $scope.credentialSettings.showCredential = !$scope.credentialSettings.showCredential;
      },

      blankCredential: function() {
        $scope.credentialSettings.credentialValue = '';
      },

      onCredentialInputKeyup: function($event) {
        if ($event.key === 'Enter') {
          $scope.saveCredential();
        } else if ($event.key === 'Escape') {
          $scope.blankCredential();
        }
      }
    });

    $scope.$watch('credentialSettings.credentialValue', function(newValue, _oldValue, scope) {
      newValue = (newValue === null || newValue === undefined) ? '' : newValue;
      if (newValue.length > 0) {
        scope.credentialSettings.saved = false;
      }
    });

    this.$onInit = function() {
      // Show the expression if it is not internal
      var currentValue = $scope.detailPaneConfig.configuration[$scope.configIndex].value;
      var internalSecret = secretUtil.isInternalSecret(
        currentValue,
        $scope.credentialSettings.store
      );
      $scope.credentialSettings.showExpression = currentValue && !internalSecret;

      api.secret.checkSecretsAvailability().then(function(res) {
        if (res.status === 200) {
          $scope.credentialSettings.useInternalSecrets = true;
          $scope.credentialSettings.store = res.data;
        } else {
          secretUtil.useOldCredentials($scope);
        }
      }, function(err) {
        secretUtil.useOldCredentials($scope);
      });
    };
  });
