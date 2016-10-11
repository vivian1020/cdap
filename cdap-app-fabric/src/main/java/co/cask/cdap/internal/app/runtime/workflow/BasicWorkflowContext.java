/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import com.google.common.collect.ImmutableMap;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of a {@link WorkflowContext}.
 */
final class BasicWorkflowContext extends AbstractContext implements WorkflowContext {

  private final WorkflowSpecification workflowSpec;
  private final WorkflowActionSpecification specification;
  private final ProgramWorkflowRunner programWorkflowRunner;
  private final WorkflowToken token;
  private final Map<String, WorkflowNodeState> nodeStates;
  private final Transactional transactional;
  private ProgramState state;

  BasicWorkflowContext(WorkflowSpecification workflowSpec, @Nullable WorkflowActionSpecification spec,
                       @Nullable ProgramWorkflowRunner programWorkflowRunner,
                       WorkflowToken token, Program program, ProgramOptions programOptions,
                       MetricsCollectionService metricsCollectionService,
                       DatasetFramework datasetFramework, TransactionSystemClient txClient,
                       DiscoveryServiceClient discoveryServiceClient, Map<String, WorkflowNodeState> nodeStates,
                       @Nullable PluginInstantiator pluginInstantiator,
                       SecureStore secureStore, SecureStoreManager secureStoreManager) {
    super(program, programOptions, (spec == null) ? new HashSet<String>() : spec.getDatasets(),
          datasetFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, Collections.singletonMap(Constants.Metrics.Tag.WORKFLOW_RUN_ID,
                                                             ProgramRunners.getRunId(programOptions).getId()),
          secureStore, secureStoreManager, pluginInstantiator);
    this.workflowSpec = workflowSpec;
    this.specification = spec;
    this.programWorkflowRunner = programWorkflowRunner;
    this.token = token;
    this.nodeStates = nodeStates;
    this.transactional = Transactions.createTransactional(getDatasetCache());
  }

  @Override
  public WorkflowSpecification getWorkflowSpecification() {
    return workflowSpec;
  }

  @Override
  public WorkflowActionSpecification getSpecification() {
    if (specification == null) {
      throw new UnsupportedOperationException("Operation not allowed.");
    }
    return specification;
  }

  @Override
  public Runnable getProgramRunner(String name) {
    if (programWorkflowRunner == null) {
      throw new UnsupportedOperationException("Operation not allowed.");
    }
    return programWorkflowRunner.create(name);
  }

  @Override
  public WorkflowToken getToken() {
    return token;
  }

  @Override
  public Map<String, WorkflowNodeState> getNodeStates() {
    return ImmutableMap.copyOf(nodeStates);
  }

  @Override
  @Deprecated
  public boolean isSuccessful() {
    return state.getStatus() == ProgramStatus.COMPLETED;
  }

  /**
   * Sets the current state of the program.
   */
  void setState(ProgramState state) {
    this.state = state;
  }

  @Override
  public ProgramState getState() {
    return state;
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    transactional.execute(runnable);
  }

  @Override
  public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
    transactional.execute(timeoutInSeconds, runnable);
  }
}
