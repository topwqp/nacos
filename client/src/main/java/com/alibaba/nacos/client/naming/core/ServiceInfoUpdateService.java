/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Service information update service.
 *
 * @author xiweng.yy
 */
public class ServiceInfoUpdateService implements Closeable {

    private static final long DEFAULT_DELAY = 1000L;

    private static final int DEFAULT_UPDATE_CACHE_TIME_MULTIPLE = 6;

    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();

    private final ServiceInfoHolder serviceInfoHolder;

    private final ScheduledExecutorService executor;

    private final NamingClientProxy namingClientProxy;

    private final InstancesChangeNotifier changeNotifier;

    public ServiceInfoUpdateService(Properties properties, ServiceInfoHolder serviceInfoHolder,
            NamingClientProxy namingClientProxy, InstancesChangeNotifier changeNotifier) {
        this.executor = new ScheduledThreadPoolExecutor(initPollingThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.naming.updater"));
        this.serviceInfoHolder = serviceInfoHolder;
        this.namingClientProxy = namingClientProxy;
        this.changeNotifier = changeNotifier;
    }

    private int initPollingThreadCount(Properties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }
        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
                UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (futureMap.get(serviceKey) != null) {
            return;
        }
        synchronized (futureMap) {
            if (futureMap.get(serviceKey) != null) {
                return;
            }

            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, groupName, clusters));
            futureMap.put(serviceKey, future);
        }
    }

    private synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop to schedule update if contain task.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void stopUpdateIfContain(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (!futureMap.containsKey(serviceKey)) {
            return;
        }
        synchronized (futureMap) {
            if (!futureMap.containsKey(serviceKey)) {
                return;
            }
            futureMap.remove(serviceKey);
        }
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    public class UpdateTask implements Runnable {

        long lastRefTime = Long.MAX_VALUE;

        private final String serviceName;

        private final String groupName;

        private final String clusters;

        private final String groupedServiceName;

        private final String serviceKey;

        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;

        public UpdateTask(String serviceName, String groupName, String clusters) {
            this.serviceName = serviceName;
            this.groupName = groupName;
            this.clusters = clusters;
            this.groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
            this.serviceKey = ServiceInfo.getKey(groupedServiceName, clusters);
        }

        @Override
        public void run() {
            long delayTime = DEFAULT_DELAY;

            try {
                //判断如果没有开启订阅，并且未发起过定时任务，直接返回
                if (!changeNotifier.isSubscribed(groupName, serviceName, clusters) && !futureMap.containsKey(serviceKey)) {
                    NAMING_LOGGER
                            .info("update task is stopped, service:" + groupedServiceName + ", clusters:" + clusters);
                    return;
                }
                //从缓存中获取服务列表信息
                ServiceInfo serviceObj = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
                if (serviceObj == null) {
                    //不存在，直接注册中心查询
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    //缓存处理，存入缓存中
                    serviceInfoHolder.processServiceInfo(serviceObj);
                    //更新最后引用时间
                    lastRefTime = serviceObj.getLastRefTime();
                    //直接返回
                    return;
                }
                // 如果缓存中存在，判断是否过期，目前来看，如果中间更新过，这个条件成立，缓存中过期了，重新拉去新的服务实例信息
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    //重新拉取新的服务实例信息
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    //更新缓存
                    serviceInfoHolder.processServiceInfo(serviceObj);
                }
                //修改缓存最后引用时间
                lastRefTime = serviceObj.getLastRefTime();
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                // TODO multiple time can be configured.
                //延迟6秒，相当于每隔6秒，计算下一次定时任务
                delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                //这里相当于已经调用远程nacos-server 拉取服务实例成功了，需要重置拉取的失败次数
                resetFailCount();
            } catch (Throwable e) {
                //从远程nacos-server拉取服务失败，失败次数加一
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + groupedServiceName, e);
            } finally {
                //延迟更新，代表延迟时间，没失败一次就延迟时间*2，最多延迟60秒，默认延迟6秒就处理
                //注意，这里是指数级避让算法实现，为了防止失败以后频繁调用nacos-server服务导致服务器压力增大
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
            }
        }

        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }

        private void resetFailCount() {
            failCount = 0;
        }
    }
}
