/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.core.IList;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.IListWriter;

import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class PeekPipeline<T> extends AbstractIntermediatePipeline<T, T> {

    private final Distributed.Consumer<? super T> consumer;

    public PeekPipeline(StreamContext context, Pipeline<T> upstream, Distributed.Consumer<? super T> consumer) {
        super(context, upstream.isOrdered(), upstream);
        this.consumer = consumer;
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        String listName = randomName();
        IList<T> list = context.getHazelcastInstance().getList(listName);
        Vertex previous = upstream.buildDAG(dag);
        Vertex writer = new Vertex(listName, IListWriter.supplier(listName));
        if (upstream.isOrdered()) {
            writer.parallelism(1);
        }
        dag.addVertex(writer);
        dag.addEdge(new Edge(previous, 1, writer, 0));
        context.addStreamListener(() -> {
            list.forEach(consumer);
            list.destroy();
        });
        return previous;
    }
}
