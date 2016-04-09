function Dag1TestCtrl(MyDagStore, $timeout) {
  this.addNode = (type) => {
    switch(type) {
      case 'source':
        this.addSource();
        break;
      case 'sink':
        this.addSink();
        break;
      case 'transform':
        this.addTransform();
        break;
    }
  };
  this.addSource = () => {
    MyDagStore.dispatch({
      name: 'Source1',
      cssClass: 'batchsource',
      endpoint: 'R',
      badgeInfo: 2,
      badgeToolTip: 'Some tooltip',
      badgeCssClass: 'text-warning',
      nodeType: 'source',
      tooltipCssClass: 'badge-warning',
      type: 'ADD-NODE'
    });
  };
  this.addSink = () => {
    MyDagStore.dispatch({
      name: 'Sink1',
      cssClass: 'batchsink',
      endpoint: 'L',
      badgeInfo: 2,
      badgeToolTip: 'Some tooltip',
      badgeCssClass: 'text-warning',
      nodeType: 'sink',
      tooltipCssClass: 'badge-warning',
      type: 'ADD-NODE'
    });
  };
  this.addTransform = () => {
    MyDagStore.dispatch({
      name: 'Transform1',
      cssClass: 'transform',
      endpoint: 'LR',
      badgeInfo: 2,
      badgeToolTip: 'Some tooltip',
      badgeCssClass: 'text-warning',
      tooltipCssClass: 'badge-warning',
      nodeType: 'transform',
      type: 'ADD-NODE'
    });
  };

  this.nodename = 'Script Filter';
  this.cssClass='transform';
  this.icon ='';
  this.endpointType = 'LR';
  this.badgeInfo = '2';
  this.badgeCssClass = 'badge-warning';
  this.badgeTooltip = 'Please check node config';
  this.tooltipCssClass = 'tooltip-warning';

  let drawDAG = () => {
    let nodes = [
      {
        'id': 'ab78832d-7962-420a-80ac-9d1de571cafc',
        'name': 'Source1',
        'endpoint': 'R',
        'icon': 'fa-plug',
        'cssClass': 'batchsource',
        'badgeInfo': 2,
        'badgeCssClass': 'text-warning',
        'badgeTooltip': null,
        'tooltipCssClass': 'badge-warning',
        'disabled': false,
        'selected': false,
        'nodeType': 'source',
        '_uiPosition': {
          'top': '120px',
          'left': '180px'
        }
      },
      {
        'id': '76783926-e310-490d-9019-f784d3dad3e6',
        'name': 'Sink1',
        'endpoint': 'L',
        'icon': 'fa-plug',
        'cssClass': 'batchsink',
        'badgeInfo': 2,
        'badgeCssClass': 'text-warning',
        'badgeTooltip': null,
        'tooltipCssClass': 'badge-warning',
        'disabled': false,
        'selected': false,
        'nodeType': 'sink',
        '_uiPosition': {
          'top': '135px',
          'left': '412px'
        }
      },
      {
        'id': '48a606f6-4b51-4010-821c-f1f8a17974b9',
        'name': 'Transform1',
        'endpoint': 'LR',
        'icon': 'fa-plug',
        'cssClass': 'transform',
        'badgeInfo': 2,
        'badgeCssClass': 'text-warning',
        'badgeTooltip': null,
        'tooltipCssClass': 'badge-warning',
        'disabled': false,
        'selected': false,
        'nodeType': 'transform',
        '_uiPosition': {
          'top': '152px',
          'left': '719px'
        }
      }
    ];
    let connections = [
      {
        'from': 'ab78832d-7962-420a-80ac-9d1de571cafc',
        'to': '48a606f6-4b51-4010-821c-f1f8a17974b9'
      },
      {
        'from': '48a606f6-4b51-4010-821c-f1f8a17974b9',
        'to': '76783926-e310-490d-9019-f784d3dad3e6'
      }
    ];
    $timeout( () => {
      MyDagStore.dispatch({
        type: 'SET-NODES',
        nodes: nodes
      });
      MyDagStore.dispatch({
        type: 'SET-CONNECTIONS',
        connections: connections
      });
      MyDagStore.dispatch({ type: 'INIT-DAG' });
    });
  };

  // drawDAG();
  
}
Dag1TestCtrl.$inject = ['MyDagStore', '$timeout'];

angular.module(`${PKG.name}.feature.experimental`)
  .controller('Dag1TestCtrl', Dag1TestCtrl);
