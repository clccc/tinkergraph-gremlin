package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import java.io.IOException;
import java.lang.ref.SoftReference;

/**
 * Softly referencing element, so that the garbage collector can clear the element if we're running
 * low on heap space. That mechanism only works if there's no strong references to the element
 * elsewhere.
 * @see org.apache.tinkerpop.gremlin.tinkergraph.storage.SoftReferenceManager
 */
public abstract class ElementRef<E extends TinkerElement> {
  public final long id;
  protected final TinkerGraph graph;
  protected SoftReference<E> softReference;

  public ElementRef(E element) {
    this.id = (long) element.id();
    this.softReference = new SoftReference<>(element);
    this.graph = (TinkerGraph) element.graph();
  }

  protected ElementRef(final long id, final TinkerGraph graph) {
    this.id = id;
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
}