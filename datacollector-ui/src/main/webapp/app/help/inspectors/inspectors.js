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

    function getAndProcessHealthReport(category) {
      category.severity = '?';
      category.open = false;

      // Run report for this inspector
      api.system.getHealthReport(category.categoryInfo.className).then(function(reportResponse) {
        category.healthChecks = reportResponse.data.categories[0].healthChecks;
        console.log(reportResponse.data);

        // By default we presume that all entries were green
        category.severity = 'GREEN';
        angular.forEach(category.healthChecks, function(check) {
          if(check.severity !== 'GREEN') {
            category.severity = 'RED';
            category.open = true;
          }
        });
      });
    }

    function parseHealthCheckCategories(response) {
      var categories = [];

      // Convert each of the inspectors into wrapping structure so that we can insert results from a separate
      // REST call a bit later on.
      angular.forEach(response.data, function(category) {
        var cat = {
          categoryInfo: category,
          severity: '?',
          open: false
        };

        categories.push(cat);
        getAndProcessHealthReport(cat);
      });

      return categories;
    }

    $q.all([
      configuration.init(),
      api.system.getHealthCheckCategories()
    ]).then(function(results) {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/inspectors');
      }
      $scope.categories = parseHealthCheckCategories(results[1]);
    });

    $scope.expandAll = function(expandOperations) {
      for (var i = 0, categories = $scope.categories, l = categories.length; i < l; i++) {
        categories[i].open = expandOperations;
      }
    };

    /**
     * Flips "open" toggle on any item given.
     */
    $scope.toggleOpen = function(item) {
      item.open = !item.open;
    };

    $scope.rerunCategory = function(category) {
      getAndProcessHealthReport(category);
    };

  });
