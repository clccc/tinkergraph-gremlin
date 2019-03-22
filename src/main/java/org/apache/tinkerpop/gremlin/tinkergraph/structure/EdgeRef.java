package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import java.io.IOException;
import java.util.Iterator;

public class EdgeRef extends ElementRef<TinkerEdge> implements Edge {

  public EdgeRef(SpecializedTinkerEdge Edge) {
    super(Edge);
  }

  public EdgeRef(final long edgeId, final TinkerGraph graph) {
    super(edgeId, graph);
  }

  @Override
  protected TinkerEdge readFromDisk(final long edgeId) throws IOException {
    return (TinkerEdge) graph.ondiskOverflow.readEdge(edgeId);
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
  public <V> Property<V> property(String key, V value) {
    return this.get().property(key, value);
  }

  @Override
  public void remove() {
    this.get().remove();
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    return this.get().vertices(direction);
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    return this.get().properties(propertyKeys);
  }
}
