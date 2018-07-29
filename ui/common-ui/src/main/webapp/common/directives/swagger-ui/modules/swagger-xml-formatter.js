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
  .service('swaggerUiXmlFormatter', ['$q', function($q) {

    function formatXml(xml) {
      var formatted = '',
        reg = /(>)(<)(\/*)/g,
        pad = 0;

      xml = xml.replace(reg, '$1\r\n$2$3');
      angular.forEach(xml.split('\r\n'), function(node) {
        var indent = 0,
          padding = '';

        if (node.match(/.+<\/\w[^>]*>$/)) {
          indent = 0;
        } else if (node.match(/^<\/\w/)) {
          if (pad !== 0) {
            pad -= 1;
          }
        } else if (node.match(/^<\w[^>]*[^\/]>.*$/)) {
          indent = 1;
        } else {
          indent = 0;
        }

        for (var i = 0; i < pad; i++) {
          padding += '    ';
        }

        formatted += padding + node + '\r\n';
        pad += indent;
      });

      return formatted;
    }

    /**
     * Module entry point
     */
    this.execute = function(response) {
      var executed = false,
        deferred = $q.defer();

      if (response.headers && response.headers()['content-type'] === 'application/xml') {
        response.data = formatXml(response.data);
        executed = true;
      }
      deferred.resolve(executed);
      return deferred.promise;
    };

  }]);