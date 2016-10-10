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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.NoTransaction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import com.google.common.base.Throwables;
import org.apache.tephra.TransactionFailureException;

/**
 * Spark Programs that are used to test explicit transactions in Spark lifecycle.
 * TODO: These is cannot be static inner classes of the {@link AppWithTimedTransactions} due to CDAP-7428
 */
public class SparkWithTimedTransactions {

  public static class TxSpark extends AbstractSpark implements JavaSparkMain {
    @Override
    protected void configure() {
      setName(AppWithTimedTransactions.SPARK_TX);
      setMainClass(TxSpark.class);
    }

    @Override
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      AppWithTimedTransactions.recordTransaction(getContext(), AppWithTimedTransactions.SPARK_TX,
                                                 AppWithTimedTransactions.INITIALIZE);
      AppWithTimedTransactions.attemptNestedTransaction(getContext(), AppWithTimedTransactions.SPARK_TX,
                                                        AppWithTimedTransactions.INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      AppWithTimedTransactions.recordTransaction(getContext(), AppWithTimedTransactions.SPARK_TX,
                                                 AppWithTimedTransactions.DESTROY);
      AppWithTimedTransactions.attemptNestedTransaction(getContext(), AppWithTimedTransactions.SPARK_TX,
                                                        AppWithTimedTransactions.DESTROY_NEST);
    }

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      // no-op
    }
  }

  public static class NoTxSpark extends TxSpark {
    @Override
    protected void configure() {
      super.configure();
      setName(AppWithTimedTransactions.SPARK_NOTX);
    }

    @Override
    @NoTransaction
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      AppWithTimedTransactions.recordTransaction(getContext(), AppWithTimedTransactions.SPARK_NOTX,
                                                 AppWithTimedTransactions.INITIALIZE);
      AppWithTimedTransactions.executeRecordTransaction(getContext(), AppWithTimedTransactions.SPARK_NOTX,
                                                        AppWithTimedTransactions.INITIALIZE_TX,
                                                        AppWithTimedTransactions.TIMEOUT_SPARK_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          AppWithTimedTransactions.attemptNestedTransaction(getContext(),
                                                            AppWithTimedTransactions.SPARK_NOTX,
                                                            AppWithTimedTransactions.INITIALIZE_NEST);
        }
      });
    }

    @Override
    @NoTransaction
    public void destroy() {
      AppWithTimedTransactions.recordTransaction(getContext(), AppWithTimedTransactions.SPARK_NOTX,
                                                 AppWithTimedTransactions.DESTROY);
      AppWithTimedTransactions.executeRecordTransaction(getContext(), AppWithTimedTransactions.SPARK_NOTX,
                                                        AppWithTimedTransactions.DESTROY_TX,
                                                        AppWithTimedTransactions.TIMEOUT_SPARK_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            AppWithTimedTransactions.attemptNestedTransaction(getContext(), AppWithTimedTransactions.SPARK_NOTX,
                                                              AppWithTimedTransactions.DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
