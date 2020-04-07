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
  * Utils shared for working with secrets and credentials
  */
angular.module('dataCollectorApp.common')
  .factory('secretUtil', function() {
    var secretUtil = {};

    var PIPELINE_VAULT_PREFIX = 'PIPELINE_VAULT_';
    var SECRET_SEPARATOR = '_';
    var USER_GROUP = 'all';
    var SEPARATOR = '/';

    /**
     * Checks if the credential EL is for the interal secret storage
     */
    secretUtil.isInternalSecret = function(credentialEL, store) {
      credentialEL = credentialEL || '';
      return credentialEL.startsWith('\${credential:get("' + store +'"') ||
        credentialEL.startsWith('\${credential:get(\'' + store +'\'');
    };

    /**
     * Updates scope.credentialSettings fields useInternalSecrets and showValue
     * to use old values before we added internal secret storage
     */
    secretUtil.useOldCredentials = function(scope) {
      scope.credentialSettings.useInternalSecrets = false;
      // Set the field to show the value if it is a credential function
      scope.credentialSettings.showValue = (
        !scope.detailPaneConfig.configuration[scope.configIndex].value ||
        scope.detailPaneConfig.configuration[scope.configIndex].value.indexOf('${')
        ) === 0;
    };

    /**
     * Gets the standard expression for a saved internal secret, plus the vaultName and secretName
     */
    secretUtil.getStandardExpression = function(pipelineId, objectInstanceName, configDefName, store) {
      var vaultName = PIPELINE_VAULT_PREFIX + pipelineId;
      var secretName = objectInstanceName + SECRET_SEPARATOR + configDefName;
      var expression =
        "\${credential:get('" + store + "', '" + USER_GROUP + "', '" + vaultName + SEPARATOR + secretName + "')}";
      return {vaultName: vaultName, secretName: secretName, expression: expression};
    };

    return secretUtil;
});
