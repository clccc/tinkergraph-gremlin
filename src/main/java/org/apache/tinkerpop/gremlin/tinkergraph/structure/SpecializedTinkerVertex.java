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

import gnu.trove.map.hash.THashMap;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.stream.StreamSupport;

public abstract class SpecializedTinkerVertex extends TinkerVertex {

    /** property keys for a specialized vertex  */
    protected abstract Set<String> specificKeys();

    public abstract Set<String> allowedOutEdgeLabels();
    public abstract Set<String> allowedInEdgeLabels();

    protected Map<String, List<Edge>> outEdgesByLabel;
    protected Map<String, List<Edge>> inEdgesByLabel;

    /** `dirty` flag for serialization to avoid superfluous serialization */
    private boolean modifiedSinceLastSerialization = true;
    private Semaphore modificationSemaphore = new Semaphore(1);

    protected SpecializedTinkerVertex(long id, String label, TinkerGraph graph) {
        super(id, label, graph);
    }

    @Override
    public Set<String> keys() {
        return specificKeys();
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
            return (Iterator) specificKeys().stream().flatMap(key ->
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
        if (!allowedOutEdgeLabels().contains(label)) {
            throw new IllegalArgumentException(getClass().getName() + " doesn't allow outgoing edges with label=" + label);
        }
        if (!((SpecializedTinkerVertex) inVertex).allowedInEdgeLabels().contains(label)) {
            throw new IllegalArgumentException(inVertex.getClass().getName() + " doesn't allow incoming edges with label=" + label);
        }
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (graph.specializedEdgeFactoryByLabel.containsKey(label)) {
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
            storeOutEdge(edge);
            ((SpecializedTinkerVertex) inVertex).storeInEdge(edge);
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

    /** do not call directly (other than from deserializer and SpecializedTinkerVertex.addEdge) */
    public void storeOutEdge(final Edge edge) {
        storeEdge(edge, getOutEdgesByLabel());
    }
    
    /** do not call directly (other than from deserializer and SpecializedTinkerVertex.addEdge) */
    public void storeInEdge(final Edge edge) {
        storeEdge(edge, getInEdgesByLabel());
    }

    private void storeEdge(final Edge edge, final Map<String, List<Edge>> edgesByLabel) {
        if (!edgesByLabel.containsKey(edge.label())) {
            // TODO ArrayLists aren't good for concurrent modification, use memory-light concurrency safe list
            edgesByLabel.put(edge.label(), new ArrayList<>());
        }
        edgesByLabel.get(edge.label()).add(edge);
    }


    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final MultiIterator<Edge> multiIterator = new MultiIterator<>();

        if (edgeLabels.length == 0) { // follow all labels
            getOutEdgesByLabel().values().forEach(edges -> multiIterator.addIterator(edges.iterator()));
            getInEdgesByLabel().values().forEach(edges -> multiIterator.addIterator(edges.iterator()));
        } else {
            for (String label : edgeLabels) {
                /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    multiIterator.addIterator(getOutEdgesByLabel().get(label).iterator());
                } else if (direction == Direction.IN || direction == Direction.BOTH) {
                    multiIterator.addIterator(getInEdgesByLabel().get(label).iterator());
                }
            }
        }
        
        return multiIterator;
    }
    
    protected Map<String, List<Edge>> getOutEdgesByLabel() {
        if (outEdgesByLabel == null) {
            this.outEdgesByLabel = new THashMap<>();
        }
        return outEdgesByLabel;
    }
    
    protected Map<String, List<Edge>> getInEdgesByLabel() {
        if (inEdgesByLabel == null) {
            this.inEdgesByLabel = new THashMap<>();
        }
        return inEdgesByLabel;
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
