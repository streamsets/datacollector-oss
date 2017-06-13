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
  .service('swaggerModules', ['$q', function($q) {

    var modules = {};

    this.BEFORE_LOAD = 'BEFORE_LOAD';
    this.BEFORE_PARSE = 'BEFORE_PARSE';
    this.PARSE = 'PARSE';
    this.BEFORE_DISPLAY = 'BEFORE_DISPLAY';
    this.BEFORE_EXPLORER_LOAD = 'BEFORE_EXPLORER_LOAD';
    this.AFTER_EXPLORER_LOAD = 'AFTER_EXPLORER_LOAD';

    /**
     * Adds a new module to swagger-ui
     */
    this.add = function(phase, module) {
      if (!modules[phase]) {
        modules[phase] = [];
      }
      if (modules[phase].indexOf(module) < 0) {
        modules[phase].push(module);
      }
    };

    /**
     * Runs modules' "execute" function one by one
     */
    function executeAll(deferred, phaseModules, args, phaseExecuted) {
      var module = phaseModules.shift();
      if (module) {
        module
          .execute.apply(module, args)
          .then(function(executed) {
            phaseExecuted = phaseExecuted || executed;
            executeAll(deferred, phaseModules, args, phaseExecuted);
          })
          .catch(deferred.reject);
      } else {
        deferred.resolve(phaseExecuted);
      }
    }

    /**
     * Executes modules' phase
     */
    this.execute = function() {
      var args = Array.prototype.slice.call(arguments), // get an Array from arguments
        phase = args.splice(0, 1),
        deferred = $q.defer(),
        phaseModules = modules[phase] || [];

      executeAll(deferred, [].concat(phaseModules), args);
      return deferred.promise;
    };

  }]);