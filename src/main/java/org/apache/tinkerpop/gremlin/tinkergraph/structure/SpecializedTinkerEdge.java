/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.Semaphore;

public abstract class SpecializedTinkerEdge extends TinkerEdge {

    /** `dirty` flag for serialization to avoid superfluous serialization */
    private boolean modifiedSinceLastSerialization = true;
    private Semaphore modificationSemaphore = new Semaphore(1);

    private final Set<String> specificKeys;

    protected SpecializedTinkerEdge(TinkerGraph graph, Long id, Vertex outVertex, String label, Vertex inVertex, Set<String> specificKeys) {
        super(graph, id, outVertex, label, inVertex);
        this.specificKeys = specificKeys;
    }

    @Override
    public Set<String> keys() {
        return specificKeys;
    }

    @Override
    public <V> Property<V> property(String key) {
        return specificProperty(key);
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract <V> Property<V> specificProperty(String key);

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        if (propertyKeys.length == 0) {
            return (Iterator) specificKeys.stream().map(key -> property(key)).filter(vp -> vp.isPresent()).iterator();
        } else if (propertyKeys.length == 1) { // treating as special case for performance
            // return IteratorUtils.of(property(propertyKeys[0]));
            final Property<V> prop = property(propertyKeys[0]);
            return prop.isPresent() ? IteratorUtils.of(prop) : Collections.emptyIterator();
        } else {
            return Arrays.stream(propertyKeys).map(key -> (Property<V>) property(key)).filter(vp -> vp.isPresent()).iterator();
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);
        final Property oldProperty = super.property(key);
        acquireModificationLock();
        modifiedSinceLastSerialization = true;
        final Property<V> p = updateSpecificProperty(key, value);
        TinkerHelper.autoUpdateIndex(this, key, value, oldProperty.isPresent() ? oldProperty.value() : null);
        releaseModificationLock();
        return p;
    }

    protected abstract <V> Property<V> updateSpecificProperty(String key, V value);

    public void removeProperty(String key) {
        acquireModificationLock();
        modifiedSinceLastSerialization = true;
        removeSpecificProperty(key);
        releaseModificationLock();
    }

    protected abstract void removeSpecificProperty(String key);

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        acquireModificationLock();
        final SpecializedTinkerVertex outVertex = (SpecializedTinkerVertex) this.outVertex();
        final SpecializedTinkerVertex inVertex = (SpecializedTinkerVertex) this.inVertex();

        Long id = (Long) this.id();
        outVertex.removeOutEdge(id);
        inVertex.removeInEdge(id);

        TinkerHelper.removeElementIndex(this);
        graph.edges.remove(id);
        if (graph.ondiskOverflowEnabled) {
            graph.ondiskOverflow.removeEdge(id);
            graph.edgeIdsByLabel.get(label()).remove(id);
            graph.onDiskEdgeOverflow.remove(id);
            graph.edgeCache.remove(id);
        }

        this.properties = null;
        this.removed = true;
        modifiedSinceLastSerialization = true;
        releaseModificationLock();
    }

    public void setModifiedSinceLastSerialization(boolean modifiedSinceLastSerialization) {
      this.modifiedSinceLastSerialization = modifiedSinceLastSerialization;
    }

    public boolean isModifiedSinceLastSerialization() {
      return modifiedSinceLastSerialization;
    }

    public void acquireModificationLock() {
      try {
        modificationSemaphore.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void releaseModificationLock() {
      modificationSemaphore.release();
    }
}
