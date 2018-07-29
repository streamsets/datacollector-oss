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
 * Controller for Error Alert.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ErrorAlertController', function ($scope, $modal) {

    angular.extend($scope, {

      /**
       * Remove Error Message.
       *
       * @param errorList
       * @param index
       */
      removeAlert: function(errorList, index) {
        errorList.splice(index, 1);
      },

      /**
       * Display stack trace in modal dialog.
       *
       * @param errorObj
       */
      showStackTrace: function (errorObj) {
        $modal.open({
          templateUrl: 'errorModalContent.html',
          controller: 'ErrorModalInstanceController',
          size: 'lg',
          backdrop: true,
          resolve: {
            errorObj: function () {
              return errorObj;
            }
          }
        });
      }
    });
  });