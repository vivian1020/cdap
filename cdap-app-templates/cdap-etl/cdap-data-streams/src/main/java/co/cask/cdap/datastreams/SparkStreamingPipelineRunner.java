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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPipelineRunner;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.streaming.CountingTranformFunction;
import co.cask.cdap.etl.spark.streaming.DStreamCollection;
import co.cask.cdap.etl.spark.streaming.DefaultStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineRunner extends SparkPipelineRunner {

  private final JavaSparkExecutionContext sec;
  private final transient JavaStreamingContext streamingContext;

  public SparkStreamingPipelineRunner(JavaSparkExecutionContext sec, JavaStreamingContext streamingContext) {
    this.sec = sec;
    this.streamingContext = streamingContext;
  }

  @Override
  protected SparkCollection<Object> getSource(final String stageName,
                                              PluginFunctionContext pluginFunctionContext) throws Exception {
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(sec.getRuntimeArguments(), sec.getLogicalStartTime(),
                                                              sec.getSecureStore(), stageName);
    StreamingSource<Object> source = sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
    StreamingContext context = new DefaultStreamingContext(stageName, sec, streamingContext);
    JavaDStream<Object> dStream = source.getStream(context)
      .transform(new CountingTranformFunction<>(stageName, sec.getMetrics(), "records.out"));
    return new DStreamCollection<>(sec, dStream);
  }
}
