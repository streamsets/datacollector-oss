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
        store: 'streamsets',
        sshPublicKey: null,
        sdcId: ''
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
      },

      isUsingInternalPublicKey: function() {
        return $scope.detailPaneConfig.configuration[$scope.configIndex].value === "${credential:get('streamsets', 'all','sdc/defaultPublicKey')}";
      },

      copySshPubKey: () => {
        const text = $scope.credentialSettings.sshPublicKey;
        // Based on https://github.com/NG-ZORRO/ng-zorro-antd/blob/master/components/core/services/nz-copy-to-clipboard.service.ts
        let copyTextArea = document.createElement('textarea');
        copyTextArea.style.all = 'unset';
        copyTextArea.style.position = 'fixed';
        copyTextArea.style.top = '0';
        copyTextArea.style.clip = 'rect(0, 0, 0, 0)';
        copyTextArea.style.whiteSpace = 'pre';
        copyTextArea.style.webkitUserSelect = 'text';
        copyTextArea.style.MozUserSelect = 'text';
        copyTextArea.style.msUserSelect = 'text';
        copyTextArea.style.userSelect = 'text';
        document.body.appendChild(copyTextArea);
        copyTextArea.value = text;
        copyTextArea.select();

        const successful = document.execCommand('copy');
        if (successful) {
          $rootScope.common.infoList = [{message: 'SSH Public Key copied to the clipboard'}];
        }
      },

      downloadSshPubKey: () => {
        const text = $scope.credentialSettings.sshPublicKey;
        const sdcId = $scope.credentialSettings.sdcId;
        const dataStr = `data:text/text;charset=utf-8,${text}`;
        const exportName = `streamsets_${sdcId}_key.pub`;
        const downloadAnchorNode = document.createElement('a');
        downloadAnchorNode.setAttribute('href', dataStr);
        downloadAnchorNode.setAttribute('download', exportName);
        document.body.appendChild(downloadAnchorNode); // required for firefox
        downloadAnchorNode.click();
        downloadAnchorNode.remove();
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

    /**
     * Checks if the uiInfo has a filename for this configuration
     * @param {Object} uiInfo
     * @param {String} configDefName
     */
    const hasFileNameForThisConfig = (uiInfo, configDefName) =>
      uiInfo && uiInfo.fileHints && uiInfo.fileHints[configDefName];

    const isUsingDefaultSshPublicKey = () => {
      return ($scope.configDefinition.name === 'sshTunnelConfig.sshPublicKey' &&
        $scope.detailPaneConfig.configuration[$scope.configIndex].value ===
          "${credential:get('streamsets', 'all','sdc/defaultPublicKey')}");
    };

    this.$onInit = () => {
      const secretIsInternal = secretUtil.isInternalSecret(
        $scope.detailPaneConfig.configuration[$scope.configIndex].value,
        $scope.credentialSettings.store
      );

      // Set shown file name if one was previously uploaded
      const uiInfo = $scope.selectedObject.uiInfo;
      const configDefName = $scope.configDefinition.name;
      if (secretIsInternal &&
          hasFileNameForThisConfig(uiInfo, configDefName)) {
        $scope.credentialSettings.uploadFile.name = uiInfo.fileHints[$scope.configDefinition.name];
      }

      // Show the expression if it is not internal
      $scope.credentialSettings.showExpression = !secretIsInternal;

      api.secret.checkSecretsAvailability().then(res => {
        $scope.credentialSettings.useInternalSecrets = true;
        if (isUsingDefaultSshPublicKey()) {
          api.secret.getSSHPublicKey().then(res => {
            $scope.credentialSettings.sshPublicKey = res.data;
            api.admin.getSdcId().then(res => {
              $scope.credentialSettings.sdcId = res.data.id;
            });
          }).catch(err => {
            console.error(err);
          });
        }
      }).catch(err => {
        secretUtil.useOldCredentials($scope);
      });
    };
  });
