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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of the service that manages access to the Secure Store,
 */
abstract class AbstractSecureStore implements SecureStore, SecureStoreManager {
  private final AuthorizerInstantiator authorizer;
  private final AuthenticationContext authenticationContext;
  private final AuthorizationEnforcer authorizationEnforcer;

  AbstractSecureStore(AuthorizerInstantiator authorizerInstantiator,
                      AuthorizationEnforcer authorizationEnforcer,
                      AuthenticationContext authenticationContext) {
    this.authorizer = authorizerInstantiator;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  abstract List<SecureStoreMetadata> list(String namespace) throws Exception;
  abstract SecureStoreData get(String namespace, String name) throws Exception;
  abstract void put(String namespace, String name, String data, String description,
                    Map<String, String> properties) throws Exception;
  abstract void delete(String namespace, String name) throws Exception;

  /**
   * Lists all the secure keys in the given namespace that the user has access to. Returns an empty list if the user
   * does not have access to the namespace or any of the keys in the namespace.
   * @param namespace Name of the namespace we want the key list for.
   * @return A list of {@link SecureKeyListEntry} for all the keys visible to the user under the given namespace.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem reading from the store.
   *
   */
  @Override
  public final List<SecureStoreMetadata> listSecureData(String namespace) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    List<SecureStoreMetadata> metadatas = list(namespace);
    List<SecureStoreMetadata> result = new ArrayList<>(metadatas.size());
    for (SecureStoreMetadata metadata : metadatas) {
      String name = metadata.getName();
      if (filter.apply(new SecureKeyId(namespace, name))) {
        result.add(metadata);
      }
    }
    return result;
  }

  /**
   * Checks if the user has access to read the secure key and returns the {@link SecureStoreData} associated
   * with the key if they do.
   * @param namespace Namespace of the key that the user is trying to read.
   * @param name This is the identifier that will be used to retrieve this element.
   * @return Data associated with the key if the user has read access.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key is not found in the store.
   * @throws IOException If there was a problem reading from the store.
   * @throws UnauthorizedException If the user does not have READ permissions on the secure key.
   */
  @Override
  public final SecureStoreData getSecureData(String namespace, String name) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter;
    filter = authorizationEnforcer.createFilter(principal);
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    if (filter.apply(secureKeyId)) {
      return get(namespace, name);
    }
    throw new UnauthorizedException(principal, Action.READ, secureKeyId);
  }

  /**
   * Puts the user provided data in the secure store, if the user has write access to the namespace. Grants the user
   * all access to the newly created entity.
   * @param namespace The namespace that this key belongs to.
   * @param name This is the identifier that will be used to retrieve this element.
   * @throws BadRequestException If the request does not contain the value to be stored.
   * @throws UnauthorizedException If the user does not have write permissions on the namespace.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws AlreadyExistsException If the key already exists in the namespace. Updating is not supported.
   * @throws IOException If there was a problem storing the key to underlying provider.
   */
  @Override
  public final synchronized void putSecureData(String namespace, String name, String value, String description,
                                         Map<String, String> properties) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    NamespaceId namespaceId = new NamespaceId(namespace);
    authorizationEnforcer.enforce(namespaceId, principal, Action.WRITE);

    if (Strings.isNullOrEmpty(value)) {
      throw new BadRequestException("The data field should not be empty. This is the data that will be stored " +
                                      "securely.");
    }
    put(namespace, name, value, description, properties);
    authorizer.get().grant(new SecureKeyId(namespace, name), principal, ImmutableSet.of(Action.ALL));
  }

  /**
   * Deletes the key if the user has ADMIN privileges to the key. Clears all the privileges associated with the key.
   * @param namespace The namespace that this key belongs to.
   * @throws UnauthorizedException If the user does not have admin privileges required to delete the secure key.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key to be deleted is not found.
   * @throws IOException If there was a problem deleting it from the underlying provider.
   */
  @Override
  public final void deleteSecureData(String namespace, String name) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    authorizationEnforcer.enforce(secureKeyId, principal, Action.ADMIN);
    delete(namespace, name);
    authorizer.get().revoke(secureKeyId);
  }
}
