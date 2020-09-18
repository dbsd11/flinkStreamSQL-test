/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.environment;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * Local Stream Environment
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class MyLocalStreamEnvironment extends StreamExecutionEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

    /** The configuration to use for the local cluster. */
    private final Configuration conf;

    public List<URL> getClasspaths() {
        return classpaths;
    }

    public void setClasspaths(List<URL> classpaths) {
        this.classpaths = classpaths;
    }

    private List<URL> classpaths = Collections.emptyList();

    /**
     * Creates a new local stream environment that uses the default configuration.
     */
    public MyLocalStreamEnvironment() {
        this(null);
    }

    /**
     * Creates a new local stream environment that configures its local executor with the given configuration.
     *
     * @param config The configuration used to configure the local executor.
     */
    public MyLocalStreamEnvironment(Configuration config) {
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, " +
                            "or running in a TestEnvironment context.");
        }

        this.conf = config == null ? new Configuration() : config;
    }

    /**
     * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
     * specified name.
     *
     * @param jobName
     *            name of the job
     * @return The result of the job execution, containing elapsed time and accumulators.
     */
    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        // transform the streaming program into a JobGraph
        StreamGraph streamGraph = getStreamGraph();
        streamGraph.setJobName(jobName);
        return execute(streamGraph);
    }

    public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {

        JobGraph jobGraph = streamGraph.getJobGraph();
        jobGraph.setClasspaths(classpaths);

        Configuration configuration = new Configuration();
        configuration.addAll(jobGraph.getJobConfiguration());

        configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "512M");
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism() * 2);

        // add (and override) the settings with what the user defined
        configuration.addAll(this.conf);

        if (!configuration.contains(RestOptions.BIND_PORT)) {
            configuration.setString(RestOptions.BIND_PORT, "0");
        }

        int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                .build();

        if (LOG.isInfoEnabled()) {
            LOG.info("Running job on local embedded Flink mini cluster");
        }

        MiniCluster miniCluster = new MiniCluster(cfg);

        try {
            miniCluster.start();
            configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

            return miniCluster.executeJobBlocking(jobGraph);
        }
        finally {
            transformations.clear();
            miniCluster.close();
        }
    }
}
