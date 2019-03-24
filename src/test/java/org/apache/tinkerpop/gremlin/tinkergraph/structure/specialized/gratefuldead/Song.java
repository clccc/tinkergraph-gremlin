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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

public class Song extends SpecializedTinkerVertex implements Serializable {
    public static final String label = "song";

    public static final String NAME = "name";
    public static final String SONG_TYPE = "songType";
    public static final String PERFORMANCES = "performances";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME, SONG_TYPE, PERFORMANCES));
    public static final Set<String> ALLOWED_OUT_EDGE_LABELS = new HashSet<>(Arrays.asList("followedBy", "sungBy", "writtenBy"));
    public static final Set<String> ALLOWED_IN_EDGE_LABELS = new HashSet<>();

    // properties
    private String name;
    private String songType;
    private Integer performances;

    public Song(Long id, TinkerGraph graph) {
        super(id, Song.label, graph);
    }

    @Override
    protected Set<String> specificKeys() {
        return SPECIFIC_KEYS;
    }

    @Override
    public Set<String> allowedOutEdgeLabels() {
        return ALLOWED_OUT_EDGE_LABELS;
    }

    @Override
    public Set<String> allowedInEdgeLabels() {
        return ALLOWED_IN_EDGE_LABELS;
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
        final VertexProperty<V> ret;
        if (NAME.equals(key) && name != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, name));
        } else if (key == SONG_TYPE && songType != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, songType));
        } else if (key == PERFORMANCES && performances != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, performances));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
        if (NAME.equals(key)) {
            this.name = (String) value;
        } else if (SONG_TYPE.equals(key)) {
            this.songType = (String) value;
        } else if (PERFORMANCES.equals(key)) {
            this.performances = (Integer) value;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
        return property(key);
    }


    @Override
    protected void removeSpecificProperty(String key) {
        if (NAME.equals(key)) {
            this.name = null;
        } else if (SONG_TYPE.equals(key)) {
            this.songType = null;
        } else if (PERFORMANCES.equals(key)) {
            this.performances = null;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
    }

    public static SpecializedElementFactory.ForVertex<Song> factory = new SpecializedElementFactory.ForVertex<Song>() {
        @Override
        public String forLabel() {
            return Song.label;
        }

        @Override
        public Song createVertex(Long id, TinkerGraph graph) {
            return new Song(id, graph);
        }
    };

    public String getName() {
        return name;
    }

    public String getSongType() {
        return songType;
    }

    public Integer getPerformances() {
        return performances;
    }
}
