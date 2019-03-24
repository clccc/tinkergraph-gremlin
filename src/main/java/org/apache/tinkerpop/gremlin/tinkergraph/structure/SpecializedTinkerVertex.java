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
import java.util.stream.StreamSupport;

public abstract class SpecializedTinkerVertex extends TinkerVertex {

    private final Set<String> specificKeys;

    /** `dirty` flag for serialization to avoid superfluous serialization */
    private boolean modifiedSinceLastSerialization = true;
    private Semaphore modificationSemaphore = new Semaphore(1);

    protected SpecializedTinkerVertex(long id, String label, TinkerGraph graph, Set<String> specificKeys) {
        super(id, label, graph);
        this.specificKeys = specificKeys;
    }

    @Override
    public Set<String> keys() {
        return specificKeys;
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        if (this.removed) return VertexProperty.empty();
        return specificProperty(key);
    }

    /* You can override this default implementation in concrete specialised instances for performance
     * if you like, since technically the Iterator isn't necessary.
     * This default implementation works fine though. */
    protected <V> VertexProperty<V> specificProperty(String key) {
        Iterator<VertexProperty<V>> iter = specificProperties(key);
        if (iter.hasNext()) {
          return iter.next();
        } else {
          return VertexProperty.empty();
        }
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract <V> Iterator<VertexProperty<V>> specificProperties(String key);

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        if (propertyKeys.length == 0) { // return all properties
            return (Iterator) specificKeys.stream().flatMap(key ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                  specificProperties(key), Spliterator.ORDERED),false)
            ).iterator();
        } else if (propertyKeys.length == 1) { // treating as special case for performance
            return specificProperties(propertyKeys[0]);
        } else {
            return (Iterator) Arrays.stream(propertyKeys).flatMap(key ->
              StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                specificProperties(key), Spliterator.ORDERED),false)
            ).iterator();
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        acquireModificationLock();
        this.modifiedSinceLastSerialization = true;
        final VertexProperty<V> vp = updateSpecificProperty(cardinality, key, value);
        TinkerHelper.autoUpdateIndex(this, key, value, null);
        releaseModificationLock();
        return vp;
    }

    protected abstract <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value);

    public void removeProperty(String key) {
        acquireModificationLock();
        modifiedSinceLastSerialization = true;
        removeSpecificProperty(key);
        releaseModificationLock();
    }

    protected abstract void removeSpecificProperty(String key);

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        }
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }

        if (graph.specializedEdgeFactoryByLabel.containsKey(label)) {
            ElementHelper.legalPropertyKeyValueArray(keyValues);

            SpecializedElementFactory.ForEdge factory = graph.specializedEdgeFactoryByLabel.get(label);
            Long idValue = (Long) graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
            if (null != idValue) {
                if (graph.edges.containsKey(idValue))
                    throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
            } else {
                idValue = (Long) graph.edgeIdManager.getNextId(graph);
            }
            graph.currentId.set(Long.max(idValue, graph.currentId.get()));

            Vertex outVertex = this;
            Edge edge = factory.createEdge(idValue, graph, outVertex, inVertex);
            ElementHelper.attachProperties(edge, keyValues);
            final Edge edgeRef;
            if (graph.ondiskOverflowEnabled) {
                edgeRef = new EdgeRef(edge);
            } else {
                edgeRef = edge;
            }
            graph.edges.put(edge.id(), edge);
            graph.getElementsByLabel(graph.edgesByLabel, label).add(edgeRef);

            acquireModificationLock();
            addSpecializedOutEdge(edge);
            ((SpecializedTinkerVertex) inVertex).addSpecializedInEdge(edge);
            releaseModificationLock();
            modifiedSinceLastSerialization = true;
            return edge;
        } else { // edge label not registered for a specialized factory, treating as generic edge
            if (graph.usesSpecializedElements) {
                throw new IllegalArgumentException(
                    "this instance of TinkerGraph uses specialized elements, but doesn't have a factory for label " + label
                        + ". Mixing specialized and generic elements is not (yet) supported");
            }
            return super.addEdge(label, inVertex, keyValues);
        }
    }

    /** do not call directly (other than from deserializer)
     *  I whish there was an easy way to forbid this in java */
    public abstract void addSpecializedOutEdge(SpecializedTinkerEdge edge);

    /** do not call directly (other than from deserializer)
     *  I whish there was an easy way to forbid this in java */
    public abstract void addSpecializedInEdge(SpecializedTinkerEdge edge);

    public abstract void removeSpecializedOutEdge(SpecializedTinkerEdge edge);

    public abstract void removeSpecializedInEdge(SpecializedTinkerEdge edge);

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        Iterator<Edge> edges = edges(direction, edgeLabels);
        if (direction == Direction.IN) {
            return IteratorUtils.map(edges, Edge::outVertex);
        } else if (direction == Direction.OUT) {
            return IteratorUtils.map(edges, Edge::inVertex);
        } else if (direction == Direction.BOTH) {
            return IteratorUtils.concat(vertices(Direction.IN, edgeLabels), vertices(Direction.OUT, edgeLabels));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public void remove() {
        super.remove();
        this.modifiedSinceLastSerialization = true;
    }

    public boolean isModifiedSinceLastSerialization() {
        return modifiedSinceLastSerialization;
    }

    public void setModifiedSinceLastSerialization(boolean modifiedSinceLastSerialization) {
        this.modifiedSinceLastSerialization = modifiedSinceLastSerialization;
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
