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
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Jason Plurad (https://github.com/pluradj)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
@Graph.OptIn("org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.TinkerGraphStrategySuite")
public final class HdfsTinkerGraph implements Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTinkerGraph.class);

    private FileSystem fs;
    private final TinkerGraph tg;
    private final String graphLocation;
    private final String graphFormat;

    /**
     * An empty private constructor that initializes {@link HdfsTinkerGraph}.
     */
    private HdfsTinkerGraph(final org.apache.commons.configuration.Configuration configuration) {
        graphLocation = configuration.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, null);

        tg = TinkerGraph.open(configuration);
        try {
            // initialize FileSystem with the Hadoop configuration
            // core-site.xml should be accessible from the classpath
            fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        } catch (IOException ioe) {
            //TODO
            fs = null;
        }

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT));

        if (graphLocation != null) loadGraph();
    }

    /**
     * Open a new {@code HdfsTinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the {@link GraphFactory} to instantiate
     * {@link Graph} instances.  This method must be overridden for the Structure Test Suite to pass. Implementers have
     * latitude in terms of how exceptions are handled within this method.  Such exceptions will be considered
     * implementation specific by the test suite as all test generate graph instances by way of
     * {@link GraphFactory}. As such, the exceptions get generalized behind that facade and since
     * {@link GraphFactory} is the preferred method to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    public static HdfsTinkerGraph open(final org.apache.commons.configuration.Configuration configuration) {
        return new HdfsTinkerGraph(configuration);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return tg.addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        return tg.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return tg.compute();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return tg.vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return tg.edges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return tg.tx();
    }

    @Override
    public void close() throws Exception {
        if (graphLocation != null) saveGraph();
    }

    @Override
    public Variables variables() {
        return tg.variables();
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).registry(TinkerIoRegistry.getInstance()).create();
    }

    @Override
    public org.apache.commons.configuration.Configuration configuration() {
        return tg.configuration();
    }

    @Override
    public Features features() {
        return tg.features();
    }
    @Override
    public String toString() {
        return String.format("hdfs%s[%s:%s]", tg.toString(), graphFormat, graphLocation);
    }

    private void loadGraph() {
        if (fs != null) {
            FSDataInputStream input = null;
            final Path f = new Path(graphLocation);
            try {
                if (!fs.exists(f)) {
                    final Path parent = f.getParent();
                    if (!fs.exists(parent)) {
                        fs.mkdirs(parent);
                    }
                } else {
                    input = fs.open(f);
                    if (fs.exists(f) && fs.isFile(f)) {
                        if (graphFormat.equals("graphml")) {
                            io(IoCore.graphml()).reader().create().readGraph(input, this);
                        } else if (graphFormat.equals("graphson")) {
                            io(IoCore.graphson()).reader().create().readGraph(input, this);
                        } else if (graphFormat.equals("gryo")) {
                            io(IoCore.gryo()).reader().create().readGraph(input, this);
                        }
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s. Cause: ", graphLocation, graphFormat, ioe.getMessage()));
            } finally {
                try {
                    if (input != null) {
                        input.close();
                    }
                } catch (IOException ioex) {
                    //TODO
                }
            }
        }
    }

    private void saveGraph() {
        if (fs != null) {
            FSDataOutputStream output = null;
            final Path f = new Path(graphLocation);
            try {
                if (!fs.exists(f)) {
                    final Path parent = f.getParent();
                    if (!fs.exists(parent)) {
                        fs.mkdirs(parent);
                    }
                }
                // this will overwrite existing file
                output = fs.create(f);

                if (graphFormat.equals("graphml")) {
                    io(IoCore.graphml()).writer().create().writeGraph(output, this);
                } else if (graphFormat.equals("graphson")) {
                    io(IoCore.graphson()).writer().create().writeGraph(output, this);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).writer().create().writeGraph(output, this);
                }
            } catch (IOException ioe) {
                throw new RuntimeException(String.format("Could not save graph at %s with %s. Cause: %s", graphLocation, graphFormat, ioe.getMessage()));
            } finally {
                try {
                    if (output != null) {
                        output.close();
                    }
                } catch (IOException ioex) {
                    //TODO
                }
            }
        }
    }
}
