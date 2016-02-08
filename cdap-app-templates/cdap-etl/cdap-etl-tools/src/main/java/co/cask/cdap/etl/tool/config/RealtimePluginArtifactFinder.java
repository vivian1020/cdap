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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.tool.ETLVersion;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSummary;

/**
 * Uses an ArtifactClient to get the artifact for batch sources and sinks.
 */
public class RealtimePluginArtifactFinder extends ClientBasedPluginArtifactFinder {

  public RealtimePluginArtifactFinder(ArtifactClient artifactClient) {
    super(artifactClient, Id.Artifact.from(Id.Namespace.DEFAULT, "cdap-etl-realtime", ETLVersion.getVersion()));
  }

  @Override
  public ArtifactSummary getSourcePluginArtifact(String pluginName) {
    return getArtifact(RealtimeSource.PLUGIN_TYPE, pluginName);
  }

  @Override
  public ArtifactSummary getSinkPluginArtifact(String pluginName) {
    return getArtifact(RealtimeSink.PLUGIN_TYPE, pluginName);
  }
}