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
  .factory('MyDAG1Factory', function() {
    var defaultSettings = {
      Connector : [ 'Flowchart', {gap: 6, stub: [10, 15], alwaysRespectStubs: true} ],
      ConnectionsDetachable: true
    };
    var connectorStyle = {
      strokeStyle: '#666e82',
      fillStyle: '#666e82',
      radius: 5,
      lineWidth: 2
    };
    function createSchemaOnEdge() {
      return angular.element('<div><div class="label-container text-center"><i class="icon-SchemaEdge"></i></div></div>');
    }
    var connectorOverlays = {
      connectorOverlays: [
        [ 'Arrow', { location: 1, length: 12, width: 12, height: 10, foldback: 1 } ],
        [ 'Custom', {
          create: createSchemaOnEdge,
          location: 0.5,
          id: 'label'
        }]
      ]
    };
    var disabledConnectorOverlays = {
      connectorOverlays: [
        [ 'Arrow', { location: 1, length: 12, width: 12, height: 10, foldback: 1 } ]
      ]
    };

    var commonSettings = {
      endpoint:'Dot',
      maxConnections: -1, // -1 means unlimited connections
      paintStyle: {
        strokeStyle: '#666e82',
        fillStyle: '#666e82',
        radius: 5,
        lineWidth: 3
      },
      anchors: [ 'Static']
    };
    var leftEndpointSettings = angular.extend({
      isSource: true,
      connectorStyle: connectorStyle,
      anchor: [ 0.5, 1, 1, 0, 26, -43, 'leftAnchor']
    }, commonSettings);
    var rightEndpointSettings = angular.extend({
      isTarget: true,
      anchor: [ 0.5, 1, -1, 0, -26, -43, 'rightAnchor'],
      connectorStyle: connectorStyle
    }, commonSettings);

    function getSettings(isDisabled) {
      var settings = {};
      if (isDisabled) {
        settings = {
          default: defaultSettings,
          commonSettings: angular.extend(commonSettings, disabledConnectorOverlays),
          leftEndpoint: angular.extend(leftEndpointSettings, disabledConnectorOverlays),
          rightEndpoint: angular.extend(rightEndpointSettings, disabledConnectorOverlays)
        };
      } else {
        settings = {
          default: defaultSettings,
          commonSettings: angular.extend(commonSettings, connectorOverlays),
          leftEndpoint: angular.extend(leftEndpointSettings, connectorOverlays),
          rightEndpoint: angular.extend(rightEndpointSettings, connectorOverlays)
        };
      }

      settings.leftLFEndpoint = angular.copy(settings.leftEndpoint);
      settings.rightLFEndpoint = angular.copy(settings.rightEndpoint);
      settings.leftLFEndpoint.anchor = [ 0.5, 1, 1, 0, 26, -43, 'leftRightAnchor'];
      settings.rightLFEndpoint.anchor = [ 0.5, 1, -1, 0, -26, -43, 'leftRightAnchor'];

      return settings;
    }

    function getIcon(plugin) {
      var iconMap = {
        'script': 'icon-script',
        'scriptfilter': 'icon-scriptfilter',
        'twitter': 'icon-twitter',
        'cube': 'icon-cube',
        'data': 'fa-database',
        'database': 'icon-database',
        'table': 'icon-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-streams',
        'jms': 'icon-jms',
        'projection': 'icon-projection',
        'amazonsqs': 'icon-amazonsqs',
        'datagenerator': 'icon-datagenerator',
        'validator': 'icon-validator',
        'corevalidator': 'corevalidator',
        'logparser': 'icon-logparser',
        'file': 'icon-file',
        'kvtable': 'icon-kvtable',
        's3': 'icon-s3',
        's3avro': 'icon-s3avro',
        's3parquet': 'icon-s3parquet',
        'snapshotavro': 'icon-snapshotavro',
        'snapshotparquet': 'icon-snapshotparquet',
        'tpfsavro': 'icon-tpfsavro',
        'tpfsparquet': 'icon-tpfsparquet',
        'sink': 'icon-sink',
        'hive': 'icon-hive',
        'structuredrecordtogenericrecord': 'icon-structuredrecord',
        'cassandra': 'icon-cassandra',
        'teradata': 'icon-teradata',
        'elasticsearch': 'icon-elasticsearch',
        'hbase': 'icon-hbase',
        'mongodb': 'icon-mongodb',
        'pythonevaluator': 'icon-pythonevaluator',
        'csvformatter': 'icon-csvformatter',
        'csvparser': 'icon-csvparser',
        'clonerecord': 'icon-clonerecord',
        'compressor': 'icon-compressor',
        'decompressor': 'icon-decompressor',
        'encoder': 'icon-encoder',
        'decoder': 'icon-decoder',
        'jsonformatter': 'icon-jsonformatter',
        'jsonparser': 'icon-jsonparser',
        'streamformatter': 'icon-streamformatter',
        'hdfs': 'icon-hdfs',
        'hasher': 'icon-hasher',
        'javascript': 'icon-javascript'

      };

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
      return icon;
    }

    function getGraphLayout(nodes, connections) {
      var graph = new dagre.graphlib.Graph();
      graph.setGraph({
        nodesep: 90,
        ranksep: 100,
        rankdir: 'LR',
        marginx: 0,
        marginy: 0
      });
      graph.setDefaultEdgeLabel(function() { return {}; });

      nodes.forEach(function (node) {
        var id = node.id || node.name;
        graph.setNode(id, { label: node.label, width: 100, height: 100 });
      });

      connections.forEach(function (connection) {
        graph.setEdge(connection.from, connection.to);
      });

      dagre.layout(graph);
      return graph;
    }

    /**
     * Utily function from jsPlumb
     * https://jsplumbtoolkit.com/community/doc/zooming.html
     *
     * slightly modified to fit our needs
     **/
    function setZoom (zoom, instance, transformOrigin, el) {
      transformOrigin = transformOrigin || [ 0.5, 0.5 ];
      el = el || instance.getContainer();
      var p = [ 'webkit', 'moz', 'ms', 'o' ],
          s = 'scale(' + zoom + ')',
          oString = (transformOrigin[0] * 100) + '% ' + (transformOrigin[1] * 100) + '%';

      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + 'Transform'] = s;
        el.style[p[i] + 'TransformOrigin'] = oString;
      }

      el.style['transform'] = s;
      el.style['transformOrigin'] = oString;

      instance.setZoom(zoom);
    }

    return {
      getSettings: getSettings,
      getIcon: getIcon,
      getGraphLayout: getGraphLayout,
      setZoom: setZoom
    };

  });
