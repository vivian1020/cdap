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

class HydratorPlusPlusCreateCanvasCtrl {
  constructor(HydratorPlusPlusBottomPanelStore, MyDagStore, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusConfigStore, HydratorPlusPlusNodeConfigActions, HydratorPlusPlusHydratorService, HydratorPlusPlusLeftPanelStore) {
    this.MyDagStore = MyDagStore;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.HydratorPlusPlusNodeConfigActions = HydratorPlusPlusNodeConfigActions;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HydratorPlusPlusLeftPanelStore = HydratorPlusPlusLeftPanelStore;

    this.setState = () => {
      this.state = {
        setScroll: (HydratorPlusPlusBottomPanelStore.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    HydratorPlusPlusBottomPanelStore.registerOnChangeListener(this.setState.bind(this));

    this.nodes = [];
    this.connections = [];

    // Listening for connectionss
    MyDagStore.subscribe(this.setStateAndUpdateConfigStore.bind(this));

  }

  setStateAndUpdateConfigStore() {
    // this.nodes = this.HydratorPlusPlusConfigStore.getNodes();
    this.connections = this.MyDagStore.getState().connections.present;
    this.HydratorPlusPlusConfigStore.setConnections(this.connections);

    let nodes = this.MyDagStore.getState().nodes.present;
    let isNodeSelected = nodes.filter( node => node.selected);
    if (isNodeSelected.length) {
      this.setActiveNode(isNodeSelected[0]);
    } else {
      this.deleteNode();
    }
  }

  setActiveNode(selectedNode) {
    var nodeId = selectedNode.id;
    if (!nodeId) {
      return;
    }
    var pluginNode;
    var nodeFromConfigStore = this.HydratorPlusPlusConfigStore.getNodes().filter( node => node.id === nodeId );
    if (nodeFromConfigStore.length) {
      pluginNode = nodeFromConfigStore[0];
    }
    this.HydratorPlusPlusNodeConfigActions.choosePlugin(pluginNode);
  }

  deleteNode() {
    this.HydratorPlusPlusNodeConfigActions.removePlugin();
  }

  generateSchemaOnEdge(sourceId) {
    return this.HydratorPlusPlusHydratorService.generateSchemaOnEdge(sourceId);
  }
}


HydratorPlusPlusCreateCanvasCtrl.$inject = ['HydratorPlusPlusBottomPanelStore', 'MyDagStore', 'DAGPlusPlusNodesActionsFactory', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusNodeConfigActions', 'HydratorPlusPlusHydratorService', 'HydratorPlusPlusLeftPanelStore'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusCreateCanvasCtrl', HydratorPlusPlusCreateCanvasCtrl);
