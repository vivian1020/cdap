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

function FlowDiagramHelper() {

  function getDAGNodes(nodes, lastNode) {
    let getIcon = (node) => {
      switch(node.type) {
        case 'STREAM':
          return 'icon-streams';
        // case 'FLOWLET':
        //   return 'icon-tigon';
        default:
          return 'fa-plug';
      }
    };
    let getEndPoint = (node, isLast) => {
      switch(node.type) {
        case 'STREAM':
          return 'R';
        default:
          return (isLast ? 'L': 'LR');
      }
    };
    let getCssClass = (node, isLast) => {
      switch(node.type) {
        case 'STREAM':
          return 'source';
        default:
          return (isLast ? 'sink' : 'transform');
      }
    };
    return nodes.map( (node) => {
      return {
        id: node.name,
        name: node.name,
        icon: getIcon(node),
        endpoint: getEndPoint(node, node.name === lastNode),
        cssClass: getCssClass(node, node.name === lastNode),
        nodeType: getCssClass(node, node.name === lastNode)
      };
    });
  }
  function getDAGConnections(edges) {
    return edges.map( edge => {
      return {
        from: edge.sourceName,
        to: edge.targetName
      };
    });
  }
  return {
    getDAGNodes,
    getDAGConnections
  };
}

angular.module(`${PKG.name}.feature.flows`)
  .factory('FlowDiagramHelper', FlowDiagramHelper);
