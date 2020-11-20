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

package com.dtstack.flink.sql.launcher.executor;

import com.dtstack.flink.sql.Main;
import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.launcher.entity.JobParamsInfo;
import com.dtstack.flink.sql.launcher.factory.StandaloneClientFactory;
import com.dtstack.flink.sql.launcher.utils.JobGraphBuildUtil;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2020/3/6
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class StandaloneExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneExecutor.class);

    JobParamsInfo jobParamsInfo;

    public StandaloneExecutor(JobParamsInfo jobParamsInfo) {
        this.jobParamsInfo = jobParamsInfo;
    }

    public void exec() throws Exception {

        Preconditions.checkArgument(StringUtils.equalsIgnoreCase(jobParamsInfo.getPluginLoadMode(), EPluginLoadMode.CLASSPATH.name()),
                "standalone only supports classpath mode");


        JobGraph jobGraph = FutureUtils.supplyAsync(() -> JobGraphBuildUtil.buildJobGraph(jobParamsInfo), ForkJoinPool.commonPool()).get();
        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir(), jobParamsInfo.getConfProperties());

        if (!StringUtils.isBlank(jobParamsInfo.getUdfJar())) {
            JobGraphBuildUtil.fillUserJarForJobGraph(jobParamsInfo.getUdfJar(), jobGraph);
        }

        JobGraphBuildUtil.fillJobGraphClassPath(jobGraph);

        Boolean isVvpRunMain = flinkConfiguration.get(ConfigOptions.key("vvp.job.run_main").booleanType().defaultValue(false).withDescription("isVvpRunMain"));
        if (BooleanUtils.isTrue(isVvpRunMain)) {
            //接入vvp，自定义job submit
            Main.main(jobParamsInfo.getExecArgs());
        } else {
            ClusterDescriptor clusterDescriptor = StandaloneClientFactory.INSTANCE.createClusterDescriptor("", flinkConfiguration);
            ClusterClientProvider clusterClientProvider = clusterDescriptor.retrieve(StandaloneClusterId.getInstance());
            ClusterClient clusterClient = clusterClientProvider.getClusterClient();


            ClusterMode execMode = ClusterMode.valueOf(jobParamsInfo.getMode());
            switch (execMode) {
                case standalone:
                    break;
                case standalonePer:
                    String fixJobId = null;
                    Field configurationField = StreamExecutionEnvironment.class.getDeclaredField("configuration");
                    configurationField.setAccessible(true);
                    Configuration configuration = (Configuration) configurationField.get(StreamExecutionEnvironment.getExecutionEnvironment());
                    if (configuration != null) {
                        fixJobId = configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
                    }

                    Field executorServiceLoaderField = StreamExecutionEnvironment.class.getDeclaredField("executorServiceLoader");
                    executorServiceLoaderField.setAccessible(true);
                    PipelineExecutorServiceLoader executorServiceLoader = (PipelineExecutorServiceLoader) executorServiceLoaderField.get(StreamExecutionEnvironment.getExecutionEnvironment());

                    if (fixJobId != null && executorServiceLoader != null && executorServiceLoader instanceof EmbeddedExecutorServiceLoader) {
                        jobGraph.setJobID(JobID.fromHexString(fixJobId));

                        Field submittedJobIdsField = EmbeddedExecutorServiceLoader.class.getDeclaredField("submittedJobIds");
                        submittedJobIdsField.setAccessible(true);
                        List<JobID> submittedJobIds = (List<JobID>) submittedJobIdsField.get(executorServiceLoader);
                        submittedJobIds.add(JobID.fromHexString(fixJobId));
                    }
                    break;
            }

            JobExecutionResult jobExecutionResult = ClientUtils.submitJob(clusterClient, jobGraph);
            String jobId = jobExecutionResult.getJobID().toString();
            LOG.info("jobID:{}", jobId);
            System.out.println("jobID:" + jobId);
        }

        Boolean vvpJobNotExit = flinkConfiguration.get(ConfigOptions.key("vvp.job.not_exit").booleanType().defaultValue(false).withDescription("vvpJobNotExit"));
        if (BooleanUtils.isTrue(vvpJobNotExit)) {
            synchronized (this) {
                wait();
            }
        }
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        PipelineExecutorServiceLoader executorServiceLoader =
                new EmbeddedExecutorServiceLoader(
                        new ArrayList<>(), new DispatcherGateway() {
                    @Override
                    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
                        return null;
                    }

                    @Override
                    public DispatcherId getFencingToken() {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
                        return null;
                    }

                    @Override
                    public String getAddress() {
                        return null;
                    }

                    @Override
                    public String getHostname() {
                        return null;
                    }
                }, new ScheduledExecutor() {
                    @Override
                    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                        return null;
                    }

                    @Override
                    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                        return null;
                    }

                    @Override
                    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
                        return null;
                    }

                    @Override
                    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                        return null;
                    }

                    @Override
                    public void execute(Runnable command) {

                    }
                });

        ContextEnvironment.setAsContext(
                executorServiceLoader,
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false,
                false);

        StreamContextEnvironment.setAsContext(
                executorServiceLoader,
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false,
                false);

        Field executorServiceLoaderField = StreamExecutionEnvironment.class.getDeclaredField("executorServiceLoader");
        executorServiceLoaderField.setAccessible(true);
        executorServiceLoader = (PipelineExecutorServiceLoader) executorServiceLoaderField.get(StreamExecutionEnvironment.getExecutionEnvironment());
        if (executorServiceLoader != null && executorServiceLoader instanceof EmbeddedExecutorServiceLoader) {
            Field submittedJobIdsField = EmbeddedExecutorServiceLoader.class.getDeclaredField("submittedJobIds");
            submittedJobIdsField.setAccessible(true);
            List<JobID> submittedJobIds = (List<JobID>) submittedJobIdsField.get(executorServiceLoader);
            submittedJobIds.add(JobID.fromByteArray(Arrays.copyOf("sadasdsaasdsadadas".getBytes(), 16)));
        }

        int i = 0;
    }
}
