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

package co.cask.cdap.explore.executor;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.http.BodyProducer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.sql.SQLException;
import java.util.List;

/**
 * BodyProducer used for returning the results of a Query, chunk by chunk.
 */
final class QueryResultsBodyProducer extends BodyProducer {

  private final ExploreService exploreService;
  private final QueryHandle handle;

  private final StringBuffer sb;

  private List<QueryResult> results;

  public QueryResultsBodyProducer(ExploreService exploreService, QueryHandle handle) {
    this.exploreService = exploreService;
    this.handle = handle;

    this.sb = new StringBuffer();
  }

  void initialize() throws HandleNotFoundException, SQLException, ExploreException {
    sb.append(AbstractQueryExecutorHttpHandler.getCSVHeaders(exploreService.getResultSchema(handle)));
    sb.append('\n');

    results = exploreService.previewResults(handle);
    if (results.isEmpty()) {
      results = exploreService.nextResults(handle, AbstractQueryExecutorHttpHandler.DOWNLOAD_FETCH_CHUNK_SIZE);
    }
  }

  @Override
  public ChannelBuffer nextChunk() throws Exception {
    if (results.isEmpty()) {
      return ChannelBuffers.EMPTY_BUFFER;
    }

    for (QueryResult result : results) {
      AbstractQueryExecutorHttpHandler.appendCSVRow(sb, result);
      sb.append('\n');
    }
    // If failed to send to client, just propagate the IOException and let netty-http to handle
    ChannelBuffer toReturn = ChannelBuffers.wrappedBuffer(sb.toString().getBytes("UTF-8"));
    sb.delete(0, sb.length());
    results = exploreService.nextResults(handle, AbstractQueryExecutorHttpHandler.DOWNLOAD_FETCH_CHUNK_SIZE);
    return toReturn;
  }

  @Override
  public void finished() throws Exception {

  }

  @Override
  public void handleError(Throwable cause) {

  }
}
