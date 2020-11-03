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

    function getAndProcessHealthReport(inspector) {
      inspector.severity = '?';
      inspector.open = false;

      // Run report for this inspector
      api.system.getHealthInspectorReport(inspector.inspectorInfo.className).then(function(reportResponse) {
        inspector.entries = reportResponse.data.results[0].entries;

        // By default we presume that all entries were green
        inspector.severity = 'GREEN';
        angular.forEach(inspector.entries, function(entry) {
          if(entry.severity !== 'GREEN') {
            inspector.severity = 'RED';
            inspector.open = true;
          }
        });
      });
    }

    function parseHealthInspectors(response) {
      var inspectors = [];

      // Convert each of the inspectors into wrapping structure so that we can insert results from a separate
      // REST call a bit later on.
      angular.forEach(response.data, function(inspector) {
        var inspector = {
          inspectorInfo: inspector,
          severity: '?',
          open: false
        };

        inspectors.push(inspector);
        getAndProcessHealthReport(inspector);
      });

      return inspectors;
    }

    $q.all([
      configuration.init(),
      api.system.getHealthInspectors()
    ]).then(function(results) {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/inspectors');
      }
      $scope.inspectors = parseHealthInspectors(results[1]);
      $scope.fetching = false;
    });

    $scope.expandAll = function(expandOperations) {
      for (var i = 0, inspectors = $scope.inspectors, l = inspectors.length; i < l; i++) {
        inspectors[i].open = expandOperations;
      }
    };

    /**
     * Flips "open" toggle on any item given.
     */
    $scope.toggleOpen = function(item) {
      item.open = !item.open;
    };

    $scope.rerunInspector = function(inspector) {
      getAndProcessHealthReport(inspector);
    };

  });
