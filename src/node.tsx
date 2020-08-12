import { JSONObject } from '@lumino/coreutils';
import { Dialog, showDialog } from '@jupyterlab/apputils';

import * as React from 'react';

export interface INodeModel extends JSONObject {
  cores: number;

  memory: string;

  architecture: string;

  python: string;
}

namespace NodeSettings {
  /**
   * The props for the ClusterScaling component.
   */
  export interface IProps {
    /**
     * The initial cluster model shown in the scaling.
     */
    initialModel: INodeModel;

    /**
     * A callback that allows the component to write state to an
     * external object.
     */
    stateEscapeHatch: (model: INodeModel) => void;
  }

  /**
   * The state for the ClusterScaling component.
   */
  export interface IState {
    /**
     * The proposed cluster model shown in the scaling.
     */
    model: INodeModel;
  }
}

export class NodeSettings extends React.Component<
  NodeSettings.IProps,
  NodeSettings.IState
> {
  constructor(props: NodeSettings.IProps) {
    super(props);
    let model: INodeModel = props.initialModel;
    this.state = { model };
  }

  componentDidUpdate(): void {
    let model: INodeModel = { ...this.state.model };
    this.props.stateEscapeHatch(model);
  }

  onPythonUpdate(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        python: (event.target as HTMLInputElement).value
      }
    });
  }

  onCoreUpdate(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        cores: parseInt((event.target as HTMLInputElement).value, 10)
      }
    });
  }

  onMemoryUpdate(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        memory: (event.target as HTMLInputElement).value
      }
    });
  }

  onArchitectureUpdate(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        architecture: (event.target as HTMLInputElement).value
      }
    });
  }

  render() {
    const model = this.state.model;
    return (
      <div>
        <div className="dask-nodeSetting-section">
          <span>Cores</span>
          <input
            value={model.cores}
            type="number"
            step="1"
            onChange={evt => {
              this.onCoreUpdate(evt);
            }}
          />
        </div>
        <div className="dask-nodeSetting-section">
          <span>Python Path</span>
          <input
            value={model.python}
            type="text"
            onChange={evt => {
              this.onPythonUpdate(evt);
            }}
          />
        </div>
        <div className="dask-nodeSetting-section">
          <span>Memory</span>
          <input
            value={model.memory}
            type="text"
            onChange={evt => {
              this.onMemoryUpdate(evt);
            }}
          />
        </div>
        <div className="dask-nodeSetting-section">
          <span>Architecture</span>
          <input
            value={model.architecture}
            type="text"
            onChange={evt => {
              this.onArchitectureUpdate(evt);
            }}
          />
        </div>
      </div>
    );
  }
}

export function showNodeConfigurationDialog(
  model: INodeModel
): Promise<INodeModel> {
  let updatedModel = { ...model };
  const escapeHatch = (update: INodeModel) => {
    updatedModel = update;
  };
  return showDialog({
    title: `Node Settings`,
    body: <NodeSettings stateEscapeHatch={escapeHatch} initialModel={model} />,
    buttons: [
      Dialog.cancelButton(),
      Dialog.okButton({ label: 'Accept Changes' })
    ]
  }).then(result => {
    if (result.button.accept) {
      return updatedModel;
    } else {
      return model;
    }
  });
}
