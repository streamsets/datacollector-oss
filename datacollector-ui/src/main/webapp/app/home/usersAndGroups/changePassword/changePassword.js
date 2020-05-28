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
 * Controller for Change Password Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ChangePasswordModalInstanceController', function ($rootScope, $scope, $modalInstance, api) {
    angular.extend($scope, {
      showLoading: false,
      isOffsetResetSucceed: false,
      userModel: {
        oldPwd: '',
        newPwd: '',
        newPwdVerif: ''
      },

      save: function() {
        if(!this.checkValues()){
          return;
        }
        $scope.operationInProgress = true;
        api.admin.changeUserPassword($rootScope.common.userName, $scope.userModel.oldPwd, $scope.userModel.newPwd)
          .then(function() {
            $rootScope.common.infoList = [{
              message: 'Password updated'
            }];
            $modalInstance.close(null);
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

      checkValues: function() {
        var oldPwd = $scope.userModel.oldPwd,
              newPwd = $scope.userModel.newPwd,
              newPwdVerif = $scope.userModel.newPwdVerif;

        $scope.common.errors = null;
        $scope.oldPwdError = $scope.newPwdError = $scope.newPwdVerifError = '';
        if(!oldPwd){
          $scope.oldPwdError = 'Please provide your current password.';
        }
        if(newPwd.length < 6 || newPwd.length > 12){
          $scope.newPwdError = 'Password must be 6 to 12 characters.';
        }
        if(newPwd.indexOf(' ') > -1){
          $scope.newPwdError = 'Password must not contain spaces.';
        }
        if(newPwd != newPwdVerif){
          $scope.newPwdVerifError = 'New Password and Verification must match.';
        }
        return $scope.newPwdError + $scope.newPwdVerifError === '';
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      },

    });
  });
