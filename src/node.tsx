import { Dialog, showDialog } from '@jupyterlab/apputils';

import * as React from 'react';

type Environments = { [name: string]: string };

type Configuration = Record<
  string,
  {
    name: string;
    value: any;
    type: string;
    options?: string[] | Environments;
    selection?: string;
  }
>;
export interface IServerModel {
  [cluster: string]: Configuration;
}

namespace NodeSettings {
  /**
   * The props for the ClusterScaling component.
   */
  export interface IProps {
    /**
     * The initial cluster model shown in the scaling.
     */
    initialModel: IServerModel;

    /**
     * A callback that allows the component to write state to an
     * external object.
     */
    stateEscapeHatch: (model: IServerModel) => void;

    /**
     * The initial cluster to configure. Otherwise selects a random initial one.
     */
    cluster?: string;
  }

  /**
   * The state for the ClusterScaling component.
   */
  export interface IState {
    /**
     * The proposed cluster model shown in the scaling.
     */
    model: IServerModel;
    cluster: string;
  }
}

export class NodeSettings extends React.Component<
  NodeSettings.IProps,
  NodeSettings.IState
> {
  constructor(props: NodeSettings.IProps) {
    super(props);
    let model: IServerModel = props.initialModel;
    let clusterName = props.cluster || Object.keys(props.initialModel)[0];
    this.state = { model, cluster: clusterName };
  }

  componentDidUpdate(): void {
    let model: IServerModel = { ...this.state.model };
    this.props.stateEscapeHatch(model);
  }

  onEntryUpdate(event: React.ChangeEvent, record: string): void {
    let updated = {
      ...this.state
    };
    updated.model[this.state.cluster][
      record
    ].value = (event.target as HTMLInputElement).value;
    this.setState(updated);
  }

  onEnvironmentEntryUpdate(event: React.ChangeEvent, record: string) {
    let updated = {
      ...this.state
    };
    let value = (event.target as HTMLInputElement).value;
    updated.model[this.state.cluster][record].selection = value;
    updated.model[this.state.cluster][record].value = (updated.model[
      this.state.cluster
    ][record].options as Environments)[value];
    this.setState(updated);
  }

  render() {
    const model = this.state.model[this.state.cluster];
    let clusterConfiguration = [];
    for (let key in model) {
      /** There's probably reuse of code here that you could avoid. */
      if (model[key].type == 'int') {
        clusterConfiguration.push(
          <div className="dask-nodeSetting-section">
            <span>{model[key].name}</span>
            <div>
              <input
                value={model[key].value}
                type="number"
                step="1"
                onChange={evt => {
                  this.onEntryUpdate(evt, key);
                }}
              />
            </div>
          </div>
        );
      } else if (model[key].type.startsWith('env')) {
        let options = [];
        let modelOptions = model[key].options as Environments;
        if (!modelOptions) throw new Error();
        for (let option in modelOptions) {
          options.push(<option value={option}>{option}</option>);
          /** Check that there isn't a prior selection */
          if (
            !this.state.model[this.state.cluster][key].selection &&
            modelOptions[option] == model[key].value
          ) {
            model[key].selection = option;
          }
        }
        /**
         * If there is completely no selection, then this must be uninitialized and we can set
         * a custom value. If the user changes this, we can fix it later.
         */
        if (!this.state.model[this.state.cluster][key].selection) {
          model[key].selection = 'custom value';
        }
        /** Use a completely invalid value parameter to ensure the safety of this */
        options.push(<option value="custom value">Custom</option>);
        clusterConfiguration.push(
          <div className="dask-nodeSetting-section">
            <span>{model[key].name}</span>
            <div>
              <select
                onChange={evt => {
                  this.onEnvironmentEntryUpdate(evt, key);
                }}
                value={model[key].selection}
              >
                {options}
              </select>
              <input
                type="text"
                value={model[key].value}
                disabled={model[key].selection != 'custom value'}
                onChange={evt => {
                  this.onEntryUpdate(evt, key);
                }}
              />
            </div>
          </div>
        );
      } else if (model[key].type == 'select') {
        let options = [];
        let modelOptions = model[key].options;
        if (!modelOptions) throw new Error();
        for (let option of modelOptions as string[]) {
          options.push(<option value={option}>{option}</option>);
        }
        clusterConfiguration.push(
          <div className="dask-nodeSetting-section">
            <span>{model[key].name}</span>
            <div>
              <select
                onChange={evt => {
                  this.onEntryUpdate(evt, key);
                }}
                value={model[key].value}
              >
                {options}
              </select>
            </div>
          </div>
        );
      } else {
        clusterConfiguration.push(
          <div className="dask-nodeSetting-section">
            <span>{model[key].name}</span>
            <input
              value={model[key].value}
              type="text"
              step="1"
              onChange={evt => {
                this.onEntryUpdate(evt, key);
              }}
            />
          </div>
        );
      }
    }
    return <div>{clusterConfiguration}</div>;
  }
}

export function showNodeConfigurationDialog(
  model: IServerModel
): Promise<IServerModel> {
  let updatedModel: IServerModel = model;
  const escapeHatch = (update: IServerModel) => {
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
