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

angular.module(`${PKG.name}.services`)
  .factory('GraphHelpers', function() {
    'use strict';

    /**
      * Purpose: convertNodesToEdgess a list of nodes to a list of connections
      * @param  [Array] of nodes
      * @return [Array] of connections (edges)
      * Should handle Action + Fork + Condition Nodes.
    */
    function convertNodesToEdges(nodes, connections) {
      var staticNodeTypes = [
       'ACTION', // from backend hence no 'NODE'
       'JOINNODE',
       'FORKNODE',
       'CONDITIONNODE',
       'CONDITIONEND'
     ];
      for (var i=0; i < nodes.length -1; i++) {

        if (staticNodeTypes.indexOf(nodes[i].nodeType) >-1 &&
            staticNodeTypes.indexOf(nodes[i+1].nodeType) > -1
          ) {
            if (nodes[i].nodeId === nodes[i+1].nodeId) {
              continue; // Don't connect the fork and join nodes of the same fork
            }
          connections.push({
            sourceName: nodes[i].program.programName + nodes[i].nodeId,
            targetName: nodes[i+1].program.programName + nodes[i+1].nodeId,
            sourceType: nodes[i].nodeType,
            targetType: nodes[i+1].nodeType
          });
        } else if (nodes[i].nodeType === 'FORK' || nodes[i].nodeType === 'CONDITION') {
          flatten(nodes[i-1], nodes[i], nodes[i+1], connections);
        }
      }
    }

    /**
      * Purpose: Flatten a source-fork-target combo to a list of connections
      * @param  [Array] of nodes
      * @param  [Array] of nodes
      * @param  [Array] of nodes
      * @return [Array] of connections

    */
    function flatten(source, fork, target, connections) {
      var branches = fork.branches,
          temp = [];

      for (var i =0; i<branches.length; i++) {
        temp = branches[i];
        if(source) {
          temp.unshift(source);
        }
        if(target) {
          temp.push(target);
        }
        convertNodesToEdges(temp, connections);
      }
    }

    /**
      Purpose: Expand a fork and convertNodesToEdges branched nodes to a list of connections
      * @param  [Array] of nodes
      * @return [Array] of connections

      * {nodeId}: will be used when constructing edges.

    */
    function expandNodes(nodes, expandedNodes) {
      var i, j;
      for(i=0; i<nodes.length; i++) {
        if (nodes[i].nodeType === 'ACTION') {
          nodes[i].label = nodes[i].program.programName;
          expandedNodes.push(nodes[i]);
        } else if (nodes[i].nodeType === 'FORK') {
          for (j=0; j<nodes[i].branches.length; j++) {
            expandedNodes.push({
              label: 'FORK',
              nodeType: 'FORKNODE',
              nodeId: 'FORK' + i,
              program: {
                programName: 'FORKNODE'
              }
            });
            expandNodes(nodes[i].branches[j], expandedNodes);
            expandedNodes.push({
              label: 'JOIN',
              nodeType: 'JOINNODE',
              nodeId: 'FORK' + i,
              program: {
                programName: 'JOINNODE'
              }
            });
          }
        } else if (nodes[i].nodeType === 'CONDITION') {
          nodes[i].branches = [nodes[i].ifBranch, nodes[i].elseBranch];
          for (j=0; j<nodes[i].branches.length; j++) {
            expandedNodes.push({
              label: 'IF',
              nodeType: 'CONDITIONNODE',
              nodeId: nodes[i].nodeId,
              program: {
                programName: nodes[i].predicateClassName
              }
            });
            expandNodes(nodes[i].branches[j], expandedNodes);
            expandedNodes.push({
              label: 'ENDIF',
              nodeType: 'CONDITIONEND',
              nodeId: nodes[i].nodeId,
              program: {
                programName: 'CONDITIONEND'
              }
            });
          }
        }
      }
    }

    function addStartAndEnd(nodes) {
      // Add Start and End nodes as semantically workflow needs to have it.
      nodes.unshift({
        type: 'START',
        nodeType: 'ACTION',
        nodeId: '',
        program: {
          programName: 'Start'
        }
      });

      nodes.push({
        label: 'end',
        type: 'END',
        nodeType: 'ACTION',
        nodeId: '',
        program: {
          programName: 'End'
        }
      });
      return nodes;
    }
    function getDAGConnections(edges) {
      return edges.map( edge => {
        return {
          from: edge.sourceName,
          to: edge.targetName
        };
      });
    }

    function getDAGNodes(nodes) {
      let getEndPoint = (type) => {
        switch(type) {
          case 'START':
            return 'R';
          case 'END':
            return 'L';
          default:
            return 'LR';
        }
      };
      let getIcon = (node) => {
        let programType = node.program.programType || node.nodeType;

        switch(programType) {
          case 'JOINNODE':
          case 'FORKNODE':
          case 'CONDITIONNODE':
          case 'CONDITIONEND':
            return 'icon-plug';
          case 'MAPREDUCE':
            return 'icon-mapreduce';
          case 'SPARK':
            return 'icon-spark';
          case 'Start':
            return 'fa-caret-right';
          case 'End':
            return 'fa-caret-left';
        }
      };
      let getIconCssClass = (node) => {
        let programType = node.program.programType || node.nodeType;
        switch(programType) {
          case 'MAPREDUCE':
          case 'SPARK':
          case 'JOINNODE':
          case 'FORKNODE':
          case 'CONDITIONNODE':
          case 'CONDITIONEND':
            return 'transform';
          case 'Start':
            return 'source';
          case 'End':
            return 'sink';
        }
      };
      let getName = (node) => {
        switch(node.nodeType) {
          case 'CONDITIONNODE':
            return 'IF';
          case 'CONDITIONEND':
            return 'ENDIF';
          default:
            return node.name;
        }
      };

      return nodes.map( node => {
        return {
          id: node.name,
          name: getName(node),
          endpoint: getEndPoint(node.type),
          icon: getIcon(node),
          cssClass: getIconCssClass(node),
          nodeType: getIconCssClass(node)
        };
      });
    }
    return {
      convertNodesToEdges,
      expandNodes,
      addStartAndEnd,
      getDAGNodes,
      getDAGConnections
    };

  });
