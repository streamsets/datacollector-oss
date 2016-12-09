/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
 * Controller for About Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('AboutModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      buildInfo: {},
      cancel: function() {
        $scope.success = {};
        $scope.zendeskSuccess = false;
        $scope.zendeskFail = false;
        $modalInstance.dismiss('cancel');
      },
      send: function() {
        var userInfo = {};
        userInfo.zendeskUsername = $scope.username;
        userInfo.zendeskToken = $scope.password;
        userInfo.priority = $scope.priority;
        userInfo.headline = $scope.headline;
        userInfo.commentText  = $scope.commentText;

        api.support.uploadZendesk(userInfo).
        success(function(res) {
          $scope.success = res;
          $scope.zendeskSuccess = true;
        }).
        error(function(data) {
          $scope.zendeskFail = true;
        });

      }
    });

    api.admin.getBuildInfo()
      .success(function(res) {
        $scope.buildInfo = res;
      })
      .error(function(data) {
        $scope.issues = [data];
      });
  });