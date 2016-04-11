/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.commons')
  .directive('myDag1', () => {
    return {
      restrict: 'E',
      templateUrl: 'dag-1/my-dag-1.html',
      controller: 'MyDag1Ctrl',
      controllerAs: 'MyDag1Ctrl',
      link: function(scope, element) {
        scope.element = element;
        scope.dagContainer = document.getElementById('dag-container');
        scope.diagramContainer = document.getElementById('diagram-container');
      }
    };
  });
