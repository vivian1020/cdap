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

class HydratorPlusPlusStudioCtrl {
  // Holy cow. Much DI. Such angular.
  constructor(HydratorPlusPlusLeftPanelStore, HydratorPlusPlusConfigActions, $stateParams, rConfig, $rootScope, $scope, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusNodeConfigStore, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusHydratorService, HydratorPlusPlusConsoleActions, rSelectedArtifact, rArtifacts, myLocalStorage, MyDagStore, $timeout, HydratorPlusPlusConfigStore) {
    // This is required because before we fireup the actions related to the store, the store has to be initialized to register for any events.

    this.myLocalStorage = myLocalStorage;
    this.myLocalStorage
        .get('hydrator++-leftpanel-isExpanded')
        .then(isExpanded => this.isExpanded = (isExpanded === false ? false : true))
        .catch( () => this.isExpanded = true);
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    $scope.$on('$destroy', () => {
      HydratorPlusPlusDetailNonRunsStore.reset();
      HydratorPlusPlusNodeConfigStore.reset();
      HydratorPlusPlusConsoleActions.resetMessages();
      MyDagStore.dispatch({
        type: 'SET-NODES',
        nodes: []
      });
      MyDagStore.dispatch({
        type: 'SET-CONNECTIONS',
        connections: []
      });
    });

    let getValidArtifact = () => {
      let isValidArtifact;
      if (rArtifacts.length) {
        isValidArtifact = rArtifacts.filter(r => r.name === rSelectedArtifact);
      }
      return isValidArtifact.length ? isValidArtifact[0]: rArtifacts[0];
    };
    HydratorPlusPlusNodeConfigStore.init();
    let artifact = getValidArtifact();
    if (rConfig) {
      if (!rConfig.artifact){
        rConfig.artifact = artifact;
      }
      HydratorPlusPlusConfigActions.initializeConfigStore(rConfig);
      let configJson = rConfig;
      if (!rConfig.__ui__) {
        configJson = HydratorPlusPlusHydratorService.getNodesAndConnectionsFromConfig(rConfig);
        configJson['__ui__'] = {
          nodes: configJson.nodes.map( (node) => {
            node.properties = node.plugin.properties;
            node.label = node.plugin.label;
            return node;
          })
        };
        configJson.config = {
          connections : configJson.connections
        };
      }
      let getEndpoint = (nodeType) => {
        switch(nodeType) {
          case 'batchsource':
          case 'realtimesource':
            return 'R';
          case 'batchsink':
          case 'realtimesink':
          case 'sparksink':
            return 'L';
          default:
            return 'LR';
        }
      };
      let getNodeType = (nodeType) => {
        switch(nodeType) {
          case 'batchsource':
          case 'realtimesource':
            return 'source';
          case 'batchsink':
          case 'realtimesink':
          case 'sparksink':
            return 'sink';
          default:
            return 'transform';
        }
      };
      let  nodes = configJson.__ui__.nodes.map( node => {
        return {
          id: node.name,
          name: node.plugin.label,
          endpoint: getEndpoint(node.type),
          icon: node.icon,
          cssClass: getNodeType(node.type),
          nodeType: getNodeType(node.type)
        };
      });
      // DAGPlusPlusNodesActionsFactory.createGraphFromConfig(configJson.__ui__.nodes, configJson.config.connections, configJson.config.comments);
      $timeout(() => {
        MyDagStore.dispatch({
          type: 'SET-NODES',
          nodes: nodes
        });
        MyDagStore.dispatch({
          type: 'SET-CONNECTIONS',
          connections: configJson.config.connections
        });
        MyDagStore.dispatch({type: 'INIT-DAG'});
      });
      HydratorPlusPlusConfigStore.setNodes(configJson.__ui__.nodes);
      HydratorPlusPlusConfigStore.setConnections(configJson.config.connections);
    } else {
      let config = {};
      config.artifact = artifact;
      HydratorPlusPlusConfigActions.initializeConfigStore(config);
    }
  }
  toggleSidebar() {
    this.isExpanded = !this.isExpanded;
    this.myLocalStorage.set('hydrator++-leftpanel-isExpanded', this.isExpanded);
  }
}

HydratorPlusPlusStudioCtrl.$inject = ['HydratorPlusPlusLeftPanelStore', 'HydratorPlusPlusConfigActions', '$stateParams', 'rConfig', '$rootScope', '$scope', 'HydratorPlusPlusDetailNonRunsStore', 'HydratorPlusPlusNodeConfigStore', 'DAGPlusPlusNodesActionsFactory', 'HydratorPlusPlusHydratorService', 'HydratorPlusPlusConsoleActions','rSelectedArtifact', 'rArtifacts', 'myLocalStorage', 'MyDagStore', '$timeout', 'HydratorPlusPlusConfigStore'];

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusStudioCtrl', HydratorPlusPlusStudioCtrl);
