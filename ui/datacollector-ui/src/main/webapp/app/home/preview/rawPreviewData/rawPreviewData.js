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
 * Controller for Preview Configuration Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RawPreviewDataModalInstanceController', function ($scope, $rootScope, $modalInstance, previewData,
                                                                 $timeout, pipelineService) {
    angular.extend($scope, {
      previewData: JSON.stringify(previewData, null, '  '),
      refreshCodemirror: false,
      snapshotsInfo: [],

      /**
       * Returns Codemirror Options
       * @param options
       * @returns {*}
       */
      getCodeMirrorOptions: function(options) {
        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), options);
      },

      /**
       * Cancel and Escape Command Handler
       */
      close: function() {
        $modalInstance.dismiss('cancel');
      }

    });

    $timeout(function() {
      $scope.refreshCodemirror = true;
    });

  });