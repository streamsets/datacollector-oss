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

// Controller for managing credential files
angular
  .module('dataCollectorApp.home')
  .controller('elSecretFileConfigurationController', function (
    $scope, api, secretUtil, $rootScope
  ) {
    angular.extend($scope, {
      credentialSettings: {
        showCredential: false,
        showExpression: false,
        useInternalSecrets: true,
        uploadFile: {},
        store: 'streamsets'
      },

      expressionValue: function(newValue) {
        if (arguments.length > 0) {
          // setter
          return $scope.detailPaneConfig.configuration[$scope.configIndex].value = newValue;
        } else {
          // getter
          const currentValue = $scope.detailPaneConfig.configuration[$scope.configIndex].value;
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
      },

      toggleShowCredential: function() {
        $scope.credentialSettings.showCredential = !$scope.credentialSettings.showCredential;
      },

      isUsingInternalPrivateKey: function() {
        return $scope.detailPaneConfig.configuration[$scope.configIndex].value === "${credential:get('streamsets', 'all','sdc/defaultPrivateKey')}";
      }
    });

    /**
     * Save the secret file, update the credential EL, and update the uiInfo with the filename
     * @param {File} uploadFile
     */
    const saveSecretFile = (uploadFile) => {
      const {vaultName, secretName, expression} = secretUtil.getStandardExpression(
        $scope.pipelineConfig.pipelineId,
        $scope.selectedObject.instanceName,
        $scope.configDefinition.name,
        $scope.credentialSettings.store
      );
      api.secret.createOrUpdateFileSecret(
        vaultName,
        secretName,
        uploadFile)
      .then((response) => {
        if (!$scope.selectedObject.uiInfo.fileHints) {
          $scope.selectedObject.uiInfo.fileHints = {};
        }
        $scope.selectedObject.uiInfo.fileHints[$scope.configDefinition.name] = uploadFile.name;
        $scope.detailPaneConfig.configuration[$scope.configIndex].value = expression;
      }).catch((err) => {
        if (err.data && err.data.messages) {
          $rootScope.common.errors = err.data.messages.map((x) => x.message);
        } else {
          $rootScope.common.errors = ['Unable to upload file'];
        }
        console.error('Could not save file', err);
      });
    };

    $scope.$watch('detailPaneConfig.configuration[configIndex].value', function(newValue, _oldValue, scope) {
      // Clear file upload if an expression is entered
      if (newValue.length > 0 && !secretUtil.isInternalSecret(newValue, $scope.credentialSettings.store)) {
        scope.credentialSettings.uploadFile = {};
        if (scope.selectedObject.uiInfo.fileHints) {
          scope.selectedObject.uiInfo.fileHints[scope.configDefinition.name] = null;
        }
      }
    });

    $scope.$watch('credentialSettings.uploadFile', (newValue) => {
        if (newValue && newValue instanceof File) {
          saveSecretFile(newValue);
        }
    });

    this.$onInit = () => {
      // Set shown file name if one was previously uploaded
      const uiInfo = $scope.selectedObject.uiInfo;
      if (secretUtil.isInternalSecret($scope.detailPaneConfig.configuration[$scope.configIndex].value, $scope.credentialSettings.store) &&
          uiInfo && uiInfo.fileHints && uiInfo.fileHints[$scope.configDefinition.name]) {
        $scope.credentialSettings.uploadFile.name = uiInfo.fileHints[$scope.configDefinition.name];
      }

      api.secret.checkSecretsAvailability().then(res => {
        if (res.status === 200) {
          $scope.credentialSettings.useInternalSecrets = true;
        } else {
          secretUtil.useOldCredentials($scope);
        }
      }, err => {
        // This is needed due to SDC-13714, but is still a good default once that is fixed
        secretUtil.useOldCredentials($scope);
      });
    };
  });
