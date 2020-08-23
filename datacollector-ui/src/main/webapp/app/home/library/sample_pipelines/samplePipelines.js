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

// Controller for Sample Pipelines Modal.
angular
  .module('dataCollectorApp.home')
  .controller('SamplePipelinesModalInstanceController', function (
    $scope, $modalInstance, $location, api, pipelineService, tracking, trackingEvent
  ) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      samplePipelines: [],
      sortColumn: 'name',
      sortReverse: true,

      openSamplePipeline: function(pipeline) {
        $modalInstance.close();
        $location.path('/collector/pipeline/' + pipeline.pipelineId).search({'samplePipeline': 'true'});
        tracking.mixpanel.track(trackingEvent.SAMPLE_PIPELINE_VIEW, {
          'Sample Pipeline ID': pipeline.pipelineId,
          'Sample Pipeline Title': pipeline.title
        });
      },

      close : function () {
        $modalInstance.close();
      }
    });

    var fetchSamplePipelines = function() {
      api.pipelineAgent.getPipelines(undefined, 'system:samplePipelines', 0, -1, 'TITLE', 'ASC', true)
        .then(
          function (res) {
            var pipelineInfoList = res.data[0];
            pipelineService.processSamplePipelines(pipelineInfoList);
            $scope.samplePipelines = pipelineInfoList;
          },
          function (res) {
            $scope.common.errors = [res.data];
          }
        );
    };

    fetchSamplePipelines();

  });
