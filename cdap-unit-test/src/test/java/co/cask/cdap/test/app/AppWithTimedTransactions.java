/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.NoTransaction;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.test.RevealingTxSystemClient;
import co.cask.cdap.test.RevealingTxSystemClient.RevealingTransaction;
import com.google.common.base.Throwables;
import org.apache.http.entity.ContentType;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * An app that starts transactions with custom timeout and validates the timeout using a custom dataset.
 * This app also has methods with @NoTransaction annotations, to validate that these don't get run inside a tx.
 * These methods will then start transactions explicitly, and attempt to nest transactions.
 *
 * This relies on TestBase to inject {@link RevealingTxSystemClient} for this test.
 */
@SuppressWarnings("WeakerAccess")
public class AppWithTimedTransactions extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AppWithTimedTransactions.class);

  private static final String NAME = "AppWithTimedTransactions";
  static final String CAPTURE = "capture";
  static final String INPUT = "input";
  static final String DEFAULT = "default";

  static final String ACTION = "TimedTxAction";
  static final String CONSUMER = "HttpContentConsumer";
  static final String FLOW = "TimedTxFlow";
  static final String FLOWLET_TX = "TxFlowlet";
  static final String FLOWLET_NOTX = "NoTxFlowlet";
  static final String PRODUCER = "HttpContentProducer";
  static final String SERVICE = "TimedTxService";
  static final String WORKER = "TimedTxWorker";
  static final String WORKFLOW = "TimedTxWorkflow";

  static final String INITIALIZE = "initialize";
  static final String INITIALIZE_TX = "initialize-tx";
  static final String INITIALIZE_NEST = "initialize-nest";
  static final String DESTROY = "destroy";
  static final String DESTROY_TX = "destroy-tx";
  static final String DESTROY_NEST = "destroy-nest";
  static final String PROCESS = "process";
  static final String PROCESS_NEST = "process-nest";
  static final String RUNTIME = "runtime";

  static final int TIMEOUT_ACTION_RUNTIME = 15;
  static final int TIMEOUT_CONSUMER_RUNTIME = 16;
  static final int TIMEOUT_FLOWLET_DESTROY = 17;
  static final int TIMEOUT_FLOWLET_INITIALIZE = 18;
  static final int TIMEOUT_PRODUCER_RUNTIME = 19;
  static final int TIMEOUT_WORKER_DESTROY = 21;
  static final int TIMEOUT_WORKER_INITIALIZE = 20;
  static final int TIMEOUT_WORKER_RUNTIME = 22;

  @Override
  public void configure() {
    setName(NAME);
    addStream(INPUT);
    createDataset(CAPTURE, TransactionCapturingTable.class);
    addWorker(new TimeoutWorker());
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE);
        addHandler(new TimeoutHandler());
      }
    });
    addWorkflow(new AbstractWorkflow() {
      @Override
      protected void configure() {
        setName(WORKFLOW);
        addAction(new TimeoutAction());
      }
    });
    addFlow(new AbstractFlow() {
      @Override
      protected void configure() {
        setName(FLOW);
        addFlowlet(FLOWLET_TX, new TxFlowlet());
        addFlowlet(FLOWLET_NOTX, new NoTxFlowlet());
        connectStream(INPUT, FLOWLET_TX);
        connect(FLOWLET_TX, FLOWLET_NOTX);
      }
    });
  }

  /**
   * Uses the provided Transactional with the given timeout, and records the timeout that the transaction
   * was actually given, or "default" if no explicit timeout was given.
   */
  private static void executeRecordTransaction(Transactional transactional,
                                               final String row, final String column, int timeout) {
    try {
      transactional.execute(timeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          recordTransaction(context, row, column);
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * If in a transaction, records the timeout that the current transaction was given, or "default" if no explicit
   * timeout was given. Otherwise does nothing.
   *
   * Note: we know whether and what explicit timeout was given, because we inject a {@link RevealingTxSystemClient},
   *       which returns a {@link RevealingTransaction} for {@link TransactionSystemClient#startShort(int)} only.
   */
  private static void recordTransaction(DatasetContext context, String row, String column) {
    TransactionCapturingTable capture = context.getDataset(CAPTURE);
    Transaction tx = capture.getTx();
    if (tx == null) {
      return;
    }
    // we cannot cast because the RevealingTransaction is not visible in the program class loader
    String value = DEFAULT;
    if ("RevealingTransaction".equals(tx.getClass().getSimpleName())) {
      int txTimeout;
      try {
        txTimeout = (int) tx.getClass().getField("timeout").get(tx);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      value = String.valueOf(txTimeout);
    }
    capture.getTable().put(new Put(row, column, value));
  }

  /**
   * Attempt to nest transactions. we expect this to fail, but we catch the exception and leave it to the
   * main test method to validate that no transaction was recorded.
   */
  private static void attemptNestedTransaction(Transactional txnl, final String row, final String key) {
    try {
      txnl.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          recordTransaction(ctext, row, key);
        }
      });
      LOG.error("Nested transaction should not have succeeded for {}:{}", row, key);
    } catch (TransactionFailureException e) {
      // expected: starting nested transaction should fail
      LOG.info("Nested transaction failed as expected for {}:{}", row, key);
    }
  }

  public static class TimeoutWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      executeRecordTransaction(context, WORKER, INITIALIZE, TIMEOUT_WORKER_INITIALIZE);
    }

    @Override
    public void run() {
      executeRecordTransaction(getContext(), WORKER, RUNTIME, TIMEOUT_WORKER_RUNTIME);
    }

    @Override
    public void destroy() {
      executeRecordTransaction(getContext(), WORKER, DESTROY, TIMEOUT_WORKER_DESTROY);
      super.destroy();
    }
  }

  public static class TimeoutHandler extends AbstractHttpServiceHandler {

    // service context does not have Transactional, no need to test lifecycle methods

    @PUT
    @Path("test")
    public HttpContentConsumer handle(HttpServiceRequest request, HttpServiceResponder responder) {
      return new HttpContentConsumer() {

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          executeRecordTransaction(transactional, CONSUMER, RUNTIME, TIMEOUT_CONSUMER_RUNTIME);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          responder.send(200, new HttpContentProducer() {

            @Override
            public ByteBuffer nextChunk(Transactional transactional) throws Exception {
              executeRecordTransaction(transactional, PRODUCER, RUNTIME, TIMEOUT_PRODUCER_RUNTIME);
              return ByteBuffer.allocate(0);
            }

            @Override
            public void onFinish() throws Exception {
            }

            @Override
            public void onError(Throwable failureCause) {
            }
          }, ContentType.TEXT_PLAIN.getMimeType());
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
        }
      };
    }
  }

  public static class TimeoutAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION);
    }

    @Override
    public void run() throws Exception {
      executeRecordTransaction(getContext(), ACTION, RUNTIME, TIMEOUT_ACTION_RUNTIME);
    }
  }

  static class TxFlowlet extends AbstractFlowlet {

    @SuppressWarnings("unused")
    private OutputEmitter<StreamEvent> out;

    @Override
    protected void configure() {
      setName(FLOWLET_TX);
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, context.getName(), INITIALIZE);
      attemptNestedTransaction(context, context.getName(), INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), getContext().getName(), DESTROY);
      attemptNestedTransaction(getContext(), getContext().getName(), DESTROY_NEST);
    }

    @ProcessInput
    public void process(StreamEvent event) {
      recordTransaction(getContext(), getContext().getName(), PROCESS);
      attemptNestedTransaction(getContext(), getContext().getName(), PROCESS_NEST);
      out.emit(event);
    }
  }

  public static class NoTxFlowlet extends AbstractFlowlet {

    @Override
    protected void configure() {
      setName(FLOWLET_NOTX);
    }

    @Override
    @NoTransaction
    public void initialize(final FlowletContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, context.getName(), INITIALIZE);
      executeRecordTransaction(context, context.getName(), INITIALIZE_TX, TIMEOUT_FLOWLET_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(context, context.getName(), INITIALIZE_NEST);
        }
      });
    }

    @Override
    @NoTransaction
    public void destroy() {
      recordTransaction(getContext(), getContext().getName(), DESTROY);
      executeRecordTransaction(getContext(), getContext().getName(), DESTROY_TX, TIMEOUT_FLOWLET_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), getContext().getName(), DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
      }
    }

    @ProcessInput
    public void process(@SuppressWarnings("UnusedParameters") StreamEvent event) {
      // no-op
    }
  }

}
