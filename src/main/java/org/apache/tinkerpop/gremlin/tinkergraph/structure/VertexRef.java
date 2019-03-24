package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import java.io.IOException;
import java.util.Iterator;

public class VertexRef extends ElementRef<Vertex> implements Vertex {

  public VertexRef(Vertex vertex) {
    super(vertex);
  }

  public VertexRef(final long vertexId, final String label, final TinkerGraph graph) {
    super(vertexId, label, graph);
  }

  @Override
  protected Vertex readFromDisk(final long vertexId) throws IOException {
    return graph.ondiskOverflow.readVertex(vertexId);
  }

  // delegate methods start
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
  // delegate methods end
}
