package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import java.io.IOException;
import java.util.Iterator;

public class EdgeRef extends ElementRef<Edge> implements Edge {

  public EdgeRef(Edge edge) {
    super(edge);
  }

  public EdgeRef(final long edgeId, final String label, final TinkerGraph graph) {
    super(edgeId, label, graph);
  }

  @Override
  protected Edge readFromDisk(final long edgeId) throws IOException {
    return graph.ondiskOverflow.readEdge(edgeId);
  }

  // delegate methods start
  @Override
  public <V> Property<V> property(String key, V value) {
    return this.get().property(key, value);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    return this.get().vertices(direction);
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    return this.get().properties(propertyKeys);
  }
  // delegate methods end
}
