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
 * JVMMetrics module for displaying JVM Metrics page content.
 */

angular
  .module('commonUI.jvmMetrics')
  .controller('ThreadDumpModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      showLoading: true,
      common: {
        errors: []
      },
      threads: [],
      sortColumn: 'threadId',
      sortReverse: false,

      close : function () {
        $modalInstance.dismiss('cancel');
      },

      refresh: function() {
        loadThreadDump();
      }
    });

    var loadThreadDump = function() {
      $scope.showLoading = true;
      $scope.threads = [];
      api.admin.getThreadDump().then(function(res) {
        $scope.showLoading = false;
        $scope.threads = res.data;
        
        angular.forEach($scope.threads, function(thread) {
          thread.threadId = thread.threadInfo.threadId;
          thread.threadName = thread.threadInfo.threadName;
          thread.threadState = thread.threadInfo.threadState;
          thread.blockedCount = thread.threadInfo.blockedCount;
          thread.waitedCount = thread.threadInfo.waitedCount;
        });
        
      }, function(res) {
        $scope.showLoading = false;
      });
    };


    loadThreadDump();

  });