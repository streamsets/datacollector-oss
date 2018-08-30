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
/*
 * Orange angular-swagger-ui - v0.2.3
 *
 * (C) 2015 Orange, all right reserved
 * MIT Licensed
 */
angular
  .module('swaggerUi')
  .directive('swaggerUi', ['$injector', function($injector) {

    return {
      restrict: 'A',
      controller: 'swaggerUiController',
      templateUrl: 'common/directives/swagger-ui/swagger-ui.tpl.html',
      scope: {
        // Swagger descriptor URL (string, required)
        url: '=',
        // Swagger descriptor parser type (string, optional, default = "auto")
        // Built-in allowed values:
        // 		"auto": (default) parser is based on response Content-Type
        //		"json": force using JSON parser
        //
        //	More types could be defined by external modules
        parser: '@?',
        // Swagger descriptor loading indicator (variables, optional)
        loading: '=?',
        // Use permalinks? (boolean, optional, default = false)
        // If true and if using $routeProvider, should set 'reloadOnSearch: false' in route
        // configuration to avoid UI being rendered multiple times
        permalinks: '=?',
        // Display API explorer (boolean, optional, default = false)
        apiExplorer: '=?',
        // Error handler (function, optional)
        errorHandler: '=?',
        // Are Swagger descriptors loaded from trusted source only ? (boolean, optional, default = false)
        // If true, it avoids using ngSanitize but consider HTML as trusted so won't be cleaned
        trustedSources: '=?'
      },
      link: function(scope) {
        // check parameters
        if (scope.permalinks && $injector.has('$route')) {
          var $route = $injector.get('$route','swaggerUi');
          if ($route.current && $route.current.$$route && $route.current.$$route.reloadOnSearch) {
            console.warn('AngularSwaggerUI: when using permalinks you should set reloadOnSearch=false in your route config to avoid UI being rebuilt multiple times');
          }
        }
        if (!scope.trustedSources && !$injector.has('$sanitize')) {
          console.warn('AngularSwaggerUI: you must use ngSanitize OR set trusted-sources=true as directive param if swagger descriptor are loaded from trusted sources');
        }
      }
    };
  }])
  .controller('swaggerUiController', ['$scope', '$http', '$location', '$q', 'swaggerClient', 'swaggerModules', 'swagger2JsonParser',
    function($scope, $http, $location, $q, swaggerClient, swaggerModules, swagger2JsonParser) {

      var swagger;

      // WARNING authentication is not implemented, please use 'api-explorer-transform' directive's param to customize API calls

      // add default Swagger parser (JSON)
      swaggerModules.add(swaggerModules.PARSE, swagger2JsonParser);

      /**
       * Load Swagger descriptor
       */
      function loadSwagger(url, callback) {
        $scope.loading = true;
        var options = {
          method: 'GET',
          url: url
        };
        swaggerModules
          .execute(swaggerModules.BEFORE_LOAD, options)
          .then(function() {
            $http(options)
              .then(callback)
              .catch(function(res) {
                onError({
                  code: res.status,
                  message: res.data
                });
              });
          })
          .catch(onError);
      }

      /**
       * Swagger descriptor has been loaded, launch parsing
       */
      function swaggerLoaded(swaggerType) {
        $scope.loading = false;
        var parseResult = {};
        // execute modules
        $scope.parser = $scope.parser || 'auto';
        swaggerModules
          .execute(swaggerModules.PARSE, $scope.parser, swaggerType, swagger, $scope.trustedSources, parseResult)
          .then(function(executed) {
            if (executed) {
              swaggerParsed(parseResult);
            } else {
              onError({
                code: 415,
                message: 'no parser found for Swagger descriptor of type ' + swaggerType + ' and version ' + swagger.swagger
              });
            }
          })
          .catch(onError);
      }

      /**
       * Swagger descriptor has parsed, launch display
       */
      function swaggerParsed(parseResult) {
        // execute modules
        swaggerModules
          .execute(swaggerModules.BEFORE_DISPLAY, parseResult)
          .then(function() {
            // display swagger UI
            $scope.infos = parseResult.infos;
            $scope.form = parseResult.form;
            $scope.resources = parseResult.resources;
          })
          .catch(onError);
      }

      function onError(error) {
        $scope.loading = false;
        if (typeof $scope.errorHandler === 'function') {
          $scope.errorHandler(error.message, error.code);
        } else {
          console.error(error.code, 'AngularSwaggerUI: ' + error.message);
        }
      }

      $scope.$watch('url', function(url) {
        //reset
        $scope.infos = {};
        $scope.resources = [];
        $scope.form = {};
        if (url && url !== '') {
          if ($scope.loading) {
            //TODO cancel current loading swagger
          }
          // load Swagger descriptor
          loadSwagger(url, function(res) {
            var data = res.data;
            var headers = res.headers;
            swagger = data;
            // execute modules
            swaggerModules
              .execute(swaggerModules.BEFORE_PARSE, url, swagger)
              .then(function() {
                var contentType = headers()['content-type'] || 'application/json',
                  swaggerType = contentType.split(';')[0];

                swaggerLoaded(swaggerType);
              })
              .catch(onError);
          });
        }
      });

      /**
       * show all resource's operations as list or as expanded list
       */
      $scope.expand = function(resource, expandOperations) {
        resource.open = true;
        for (var i = 0, op = resource.operations, l = op.length; i < l; i++) {
          op[i].open = expandOperations;
        }
      };

      $scope.permalink = function(name) {
        if ($scope.permalinks) {
          $location.search('swagger', name);
        }
      };

      /**
       * sends a sample API request
       */
      $scope.submitExplorer = function(operation) {
        operation.loading = true;
        swaggerClient
          .send(swagger, operation, $scope.form[operation.id])
          .then(function(result) {
            operation.loading = false;
            operation.explorerResult = result;
          });
      };

    }
  ])
  .directive('fileInput', function() {
    // helper to be able to retrieve HTML5 File in ngModel from input
    return {
      restrict: 'A',
      require: 'ngModel',
      link: function(scope, element, attr, ngModel) {
        element.bind('change', function() {
          scope.$apply(function() {
            //TODO manage multiple files ?
            ngModel.$setViewValue(element[0].files[0]);
          });
        });
      }
    };
  });
