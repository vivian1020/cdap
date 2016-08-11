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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In Memory store to facilitate testing. This stores the key value in a HashMap and provides no safety.
 * This is only meant to be used in unit testing.
 */
public class InMemorySecureStore implements SecureStore, SecureStoreManager {
  private static final String SCHEME_NAME = "jceks";
  /** Separator between the namespace name and the key name */
  private static final String NAME_SEPARATOR = ":";

  private final Map store;

  public InMemorySecureStore() {
    this.store = new HashMap();
  }

  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws Exception {
    return null;
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    return null;
  }

  @Override
  public void putSecureData(String namespace, String name, String data, String description, Map<String, String> properties) throws Exception {

  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {

  }
}
