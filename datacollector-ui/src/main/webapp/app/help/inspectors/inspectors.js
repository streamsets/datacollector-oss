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
 * SDC RESTful API module.
 */

angular
  .module('dataCollectorApp.inspectors')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/inspectors',
      {
        templateUrl: 'app/help/inspectors/inspectors.tpl.html',
        controller: 'InspectorController',
        data: {
          authorizedRoles: ['admin', 'creator', 'manager', 'guest']
        },
        reloadOnSearch: false
      }
    );
  }])
  .controller('InspectorController', function ($scope, $q, configuration, Analytics, api) {
    $scope.fetching = true;

    /**
     * Aggregating the check status to Inspector level
     * Check if any of the checks are not green
     * If so mark the inspector red.
     * If all checks are green mark it as green
     **/
    function parseHealthResults(response) {
      for (var i = 0, inspectors = response.data.checks, l1 = response.data.checks.length; i < l1; i++) {
        var hasError = false;
        for (var j = 0, checks = inspectors[i].entries, l2 = inspectors[i].entries.length; j < l2; j++) {
          if (checks[j].severity !== 'GREEN') {
            hasError = true;
            break;
          }
        }
        inspectors[i].severity = hasError ? 'RED' : 'GREEN';
        // auto expand the inspector with the error
        inspectors[i].open = hasError;
      }

      return inspectors;
    }

    $q.all([
      configuration.init(),
      api.system.getHealthInspectorsStatus()
    ]).then(function(results) {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/inspectors');
      }
      $scope.inspectors = parseHealthResults(results[1]);
      $scope.fetching = false;
    });


    $scope.expandAll = function(expandOperations) {
      for (var i = 0, inspectors = $scope.inspectors, l = inspectors.length; i < l; i++) {
        inspectors[i].open = expandOperations;
      }
    };

    $scope.expand = function(resource, expandOperations) {
      resource.open = true;
      for (var i = 0, entries = resource.entries, l = entries.length; i < l; i++) {
        entries[i].open = expandOperations;
      }
    };
  });
