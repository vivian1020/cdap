let _uuid;
let getNode = (action) => {
  return {
    id: action.id || _uuid.v4(),
    name: action.name || 'no-name',
    endpoint: action.endpoint || 'LR',
    icon: action.icon || 'fa-plug',
    cssClass: action.cssClass || '',
    badgeInfo: action.badgeInfo || null,
    badgeCssClass: action.badgeCssClass || 'badge-info',
    badgeTooltip: action.badgeTooltip || null,
    tooltipCssClass: action.tooltipCssClass || '',
    disabled: action.disabled || false,
    selected: action.selected || false,
    nodeType: action.nodeType,
    _uiPosition: {
      left: '',
      top: ''
    }
  };
};
let nodes = (state = [], action = {}) => {
  switch(action.type) {
    case 'ADD-NODE':
      return [
        ...state,
        getNode(action)
      ];
    case 'REMOVE-NODE':
      return state.filter(node => node.id !== action.id);
    case 'RESET-SELECTED':
      return state.map( node => {
        node.selected = false;
        return node;
      });
    case 'UPDATE-NODE':
      let matchIndex;
      let matchNode = state.filter( (node, index) =>{
        if (node.id === action.id) {
          matchIndex = index;
          return true;
        }
      });
      if (matchNode && matchNode.length) {
        matchNode = matchNode[0];
        angular.extend(matchNode, action.config);
        return [
          ...state.slice(0, matchIndex),
          state[matchIndex],
          ...state.slice(matchIndex+1)
        ];
      } else {
        return state;
      }
      break;
    case 'SET-NODES':
      let nodes = action.nodes.map(getNode);
      return [
        ...state,
        ...nodes
      ];
    default:
      return state;
  }
};
let connections = (state = [], action={}) => {
  switch(action.type) {
    case 'SET-CONNECTIONS':
      return [
        ...action.connections
      ];
    default:
      return state;
  }
};

let Store = (Redux, uuid) => {
  _uuid = uuid;
  let combinedReducer = Redux.combineReducers({
    nodes,
    connections
  });
  return Redux.createStore(combinedReducer);
};
Store.$inject = ['Redux', 'uuid'];

angular.module(PKG.name + '.commons')
  .factory('MyDagStore', Store);
