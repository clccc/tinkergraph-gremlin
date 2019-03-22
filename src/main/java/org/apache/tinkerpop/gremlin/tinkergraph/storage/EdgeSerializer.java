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
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.Map;

public class EdgeSerializer extends Serializer<TinkerEdge> {

  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel;

  public EdgeSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel) {
    this.graph = graph;
    this.edgeFactoryByLabel = edgeFactoryByLabel;
  }

  @Override
  public byte[] serialize(TinkerEdge edge) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    packer.packLong((Long) edge.id());
    packer.packString(edge.label());
    packProperties(packer, edge.properties());
    packer.packLong((Long) edge.outVertex().id());
    packer.packLong((Long) edge.inVertex().id());

    return packer.toByteArray();
  }


  @Override
  public TinkerEdge deserialize(byte[] bytes) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    Long id = unpacker.unpackLong();
    String label = unpacker.unpackString();
    Object[] keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());
    long outVertexId = unpacker.unpackLong();
    long inVertexId = unpacker.unpackLong();
    VertexRef outVertexRef = new VertexRef(outVertexId, graph);
    VertexRef inVertexRef = new VertexRef(inVertexId, graph);

    // TODO support generic edges too
    SpecializedTinkerEdge edge = edgeFactoryByLabel.get(label).createEdge(id, graph, outVertexRef, inVertexRef);
    ElementHelper.attachProperties(edge, keyValues);

    edge.setModifiedSinceLastSerialization(false);
    return edge;
  }
}
