package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import java.io.IOException;
import java.lang.ref.SoftReference;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Softly referencing element, so that the garbage collector can clear the element if we're running
 * low on heap space. That mechanism only works if there's no strong references to the element
 * elsewhere.
 * @see org.apache.tinkerpop.gremlin.tinkergraph.storage.SoftReferenceManager
 */
public abstract class ElementRef<E extends Element> implements Element {
  public final long id;
  public final String label;

  protected final TinkerGraph graph;
  protected SoftReference<E> softReference;

  public ElementRef(E element) {
    this.id = (long) element.id();
    this.label = element.label();
    this.graph = (TinkerGraph) element.graph();
    this.softReference = new SoftReference<>(element);
  }

  protected ElementRef(final long id, final String label, final TinkerGraph graph) {
    this.id = id;
    this.label = label;
    this.graph = graph;
  }

  public E get() {
    final E fromSoftRef = softReference.get();
    if (fromSoftRef != null) {
      return fromSoftRef;
    } else {
      try {
        final E element = readFromDisk(id);
        this.softReference = new SoftReference<>(element);
        return element;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected abstract E readFromDisk(long elementId) throws IOException;

  @Override
  public Object id() {
    return id;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public String label() {
    return label;
  }

  // delegate methods start

  @Override
  public void remove() {
    this.get().remove();
  }

  @Override
  public int hashCode() {
    return get().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return get().equals(obj);
  }
  // delegate methods end

}