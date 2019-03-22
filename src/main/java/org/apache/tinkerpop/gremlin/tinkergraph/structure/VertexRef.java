package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import java.io.IOException;
import java.util.Iterator;

public class VertexRef extends ElementRef<TinkerVertex> implements Vertex {

  public VertexRef(SpecializedTinkerVertex vertex) {
    super(vertex);
  }

  public VertexRef(final long vertexId, final TinkerGraph graph) {
    super(vertexId, graph);
  }

  @Override
  protected TinkerVertex readFromDisk(final long vertexId) throws IOException {
    return (TinkerVertex) graph.ondiskOverflow.readVertex(vertexId);
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    return this.get().addEdge(label, inVertex, keyValues);
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    return this.get().property(cardinality, key, value, keyValues);
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    return this.get().properties(propertyKeys);
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    return this.get().edges(direction, edgeLabels);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return this.get().vertices(direction, edgeLabels);
  }

  @Override
  public Object id() {
    return this.get().id();
  }

  @Override
  public String label() {
    return this.get().label();
  }

  @Override
  public Graph graph() {
    return this.get().graph();
  }

  @Override
  public void remove() {
    this.get().remove();
  }
}
