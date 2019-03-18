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
  .service('swaggerClient', ['$q', '$http', 'swaggerModules', function($q, $http, swaggerModules) {

    var baseUrl;

    /**
     * format API explorer response before display
     */
    function formatResult(deferred, response) {
      var query = '',
        data = response.data,
        config = response.config;

      if (config.params) {
        var parts = [];
        for (var key in config.params) {
          parts.push(key + '=' + encodeURIComponent(config.params[key]));
        }
        if (parts.length > 0) {
          query = '?' + parts.join('&');
        }
      }
      deferred.resolve({
        url: config.url + query,
        response: {
          body: data ? (angular.isString(data) ? data : angular.toJson(data, true)) : 'no content',
          status: response.status,
          headers: angular.toJson(response.headers(), true)
        }
      });
    }

    /**
     * Send API explorer request
     */
    this.send = function(swagger, operation, values) {
      var deferred = $q.defer(),
        query = {},
        headers = {},
        path = operation.path;

      // build request parameters
      for (var i = 0, params = operation.parameters || [], l = params.length; i < l; i++) {
        //TODO manage 'collectionFormat' (csv etc.) !!
        var param = params[i],
          value = values[param.name];

        switch (param.in) {
          case 'query':
            if (!!value) {
              query[param.name] = value;
            }
            break;
          case 'path':
            path = path.replace('{' + param.name + '}', encodeURIComponent(value));
            break;
          case 'header':
            if (!!value) {
              headers[param.name] = value;
            }
            break;
          case 'formData':
            values.body = values.body || new FormData();
            if (!!value) {
              if (param.type === 'file') {
                values.contentType = undefined; // make browser defining it by himself
              }
              values.body.append(param.name, value);
            }
            break;
          case 'body':
            values.body = values.body || value || 'null';
            break;
        }
      }

      // add headers
      headers.Accept = values.responseType;
      headers['Content-Type'] = values.body ? values.contentType : 'text/plain';

      // build request
      var options = {
          method: operation.httpMethod,
          url: [swagger.basePath || '', path].join(''),
          headers: headers,
          data: values.body,
          params: query
        },
        callback = function(response) {
          // execute modules
          swaggerModules
            .execute(swaggerModules.AFTER_EXPLORER_LOAD, response)
            .then(function() {
              formatResult(deferred, response);
            });
        };

      // execute modules
      swaggerModules
        .execute(swaggerModules.BEFORE_EXPLORER_LOAD, options)
        .then(function() {
          // send request
          $http(options)
            .then(callback)
            .catch(callback);
        });

      return deferred.promise;
    };

  }]);
