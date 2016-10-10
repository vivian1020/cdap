/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.flow.flowlet.Callback;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This class represents lifecycle of a {@link Flowlet}, Start, Stop, Suspend and Resume.
 */
final class FlowletRuntimeService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletRuntimeService.class);

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final Collection<? extends ProcessSpecification<?>> processSpecs;
  private final Callback txCallback;
  private final DataFabricFacade dataFabricFacade;
  private final Service serviceHook;

  private FlowletProcessDriver flowletProcessDriver;

  FlowletRuntimeService(Flowlet flowlet, BasicFlowletContext flowletContext,
                        Collection<? extends ProcessSpecification<?>> processSpecs,
                        Callback txCallback, DataFabricFacade dataFabricFacade,
                        Service serviceHook) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.processSpecs = processSpecs;
    this.txCallback = txCallback;
    this.dataFabricFacade = dataFabricFacade;
    this.serviceHook = serviceHook;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(flowletContext.getLoggingContext());
    flowletContext.getProgramMetrics().increment("process.instance", 1);
    flowletProcessDriver = new FlowletProcessDriver(flowletContext, dataFabricFacade, txCallback, processSpecs);

    serviceHook.startAndWait();
    initFlowlet();
    flowletProcessDriver.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    LoggingContextAccessor.setLoggingContext(flowletContext.getLoggingContext());
    if (flowletProcessDriver != null) {
      stopService(flowletProcessDriver);
    }
    destroyFlowlet();
    stopService(serviceHook);
  }

  /**
   * Suspend the running of flowlet. This method will block until the flowlet process is stopped.
   * This method only get called from FlowletProgramController, and controller handled state transition and
   * make sure thread safety.
   */
  void suspend() {
    flowletProcessDriver.stopAndWait();

    // After a FlowletProcessDriver stopped, it cannot be started again
    // Hence copying all states to a new instance and start it again on resuming.
    flowletProcessDriver = new FlowletProcessDriver(flowletProcessDriver);
  }

  /**
   * Resume the running of flowlet.
   * This method only get called from FlowletProgramController, and controller handled state transition and
   * make sure thread safety.
   */
  void resume() {
    flowletProcessDriver.startAndWait();
  }

  private void initFlowlet() throws InterruptedException {
    LOG.info("Initializing flowlet: " + flowletContext);
    TransactionExecutor.Subroutine sub = new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader classLoader = setContextCombinedClassLoader();
        try {
          flowlet.initialize(flowletContext);
        } finally {
          ClassLoaders.setContextClassLoader(classLoader);
        }
      }
    };
    try {
      if (Transactions.isTransactional(flowlet, "initialize", FlowletContext.class)) {
        try {
          dataFabricFacade.createTransactionExecutor().execute(sub);
        } catch (TransactionFailureException e) {
          throw e.getCause() == null ? e : e.getCause();
        }
      } else {
        sub.apply();
      }
      LOG.info("Flowlet initialized: " + flowletContext);
    } catch (Throwable cause) {
      LOG.error("Flowlet throws exception during flowlet initialize: " + flowletContext, cause);
      throw Throwables.propagate(cause);
    }
  }

  private void destroyFlowlet() {
    LOG.info("Destroying flowlet: " + flowletContext);
    TransactionExecutor.Subroutine sub = new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader classLoader = setContextCombinedClassLoader();
        try {
          flowlet.destroy();
        } finally {
          ClassLoaders.setContextClassLoader(classLoader);
        }
      }
    };
    try {
      if (Transactions.isTransactional(flowlet, "destroy")) {
        try {
          dataFabricFacade.createTransactionExecutor().execute(sub);
        } catch (TransactionFailureException e) {
          throw e.getCause() == null ? e : e.getCause();
        }
      } else {
        sub.apply();
      }
      LOG.info("Flowlet destroyed: " + flowletContext);
    } catch (InterruptedException e) {
      // No need to propagate, as it is shutting down.
    } catch (Throwable cause) {
      LOG.error("Flowlet throws exception during flowlet destroy: " + flowletContext, cause);
      throw Throwables.propagate(cause);
    }
  }

  /**
   * Stops the given service and wait for the completion. If there is exception, just log.
   */
  private void stopService(Service service) {
    try {
      service.stopAndWait();
    } catch (Throwable t) {
      LOG.warn("Exception when stopping service {}", service);
    }
  }


  private ClassLoader setContextCombinedClassLoader() {
    return ClassLoaders.setContextClassLoader(new CombineClassLoader(
      null, ImmutableList.of(flowletContext.getProgram().getClassLoader(), getClass().getClassLoader())));
  }
}
