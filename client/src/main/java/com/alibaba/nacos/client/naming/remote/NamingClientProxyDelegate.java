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

package com.alibaba.nacos.client.naming.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.ServerListManager;
import com.alibaba.nacos.client.naming.core.ServiceInfoUpdateService;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientManager;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Delegate of naming client proxy.
 *
 * @author xiweng.yy
 */
public class NamingClientProxyDelegate implements NamingClientProxy {

    private final long securityInfoRefreshIntervalMills = TimeUnit.SECONDS.toMillis(5);

    private final ServerListManager serverListManager;

    private final ServiceInfoUpdateService serviceInfoUpdateService;

    private final ServiceInfoHolder serviceInfoHolder;

    private final NamingHttpClientProxy httpClientProxy;

    private final NamingGrpcClientProxy grpcClientProxy;

    private final SecurityProxy securityProxy;

    private ScheduledExecutorService executorService;

    public NamingClientProxyDelegate(String namespace, ServiceInfoHolder serviceInfoHolder, Properties properties,
            InstancesChangeNotifier changeNotifier) throws NacosException {
        this.serviceInfoUpdateService = new ServiceInfoUpdateService(properties, serviceInfoHolder, this,
                changeNotifier);
        this.serverListManager = new ServerListManager(properties);
        this.serviceInfoHolder = serviceInfoHolder;
        this.securityProxy = new SecurityProxy(properties, NamingHttpClientManager.getInstance().getNacosRestTemplate());
        initSecurityProxy();
        this.httpClientProxy = new NamingHttpClientProxy(namespace, securityProxy, serverListManager, properties,
                serviceInfoHolder);
        this.grpcClientProxy = new NamingGrpcClientProxy(namespace, securityProxy, serverListManager, properties,
                serviceInfoHolder);
    }

    private void initSecurityProxy() {
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r);
            t.setName("com.alibaba.nacos.client.naming.security");
            t.setDaemon(true);
            return t;
        });
        this.securityProxy.login(serverListManager.getServerList());
        this.executorService.scheduleWithFixedDelay(() -> securityProxy.login(serverListManager.getServerList()), 0,
                securityInfoRefreshIntervalMills, TimeUnit.MILLISECONDS);
    }

    @Override
    public void registerService(String serviceName, String groupName, Instance instance) throws NacosException {
        getExecuteClientProxy(instance).registerService(serviceName, groupName, instance);
    }

    @Override
    public void deregisterService(String serviceName, String groupName, Instance instance) throws NacosException {
        getExecuteClientProxy(instance).deregisterService(serviceName, groupName, instance);
    }

    @Override
    public void updateInstance(String serviceName, String groupName, Instance instance) throws NacosException {

    }

    @Override
    public ServiceInfo queryInstancesOfService(String serviceName, String groupName, String clusters, int udpPort,
            boolean healthyOnly) throws NacosException {
        return grpcClientProxy.queryInstancesOfService(serviceName, groupName, clusters, udpPort, healthyOnly);
    }

    @Override
    public Service queryService(String serviceName, String groupName) throws NacosException {
        return null;
    }

    @Override
    public void createService(Service service, AbstractSelector selector) throws NacosException {

    }

    @Override
    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        return false;
    }

    @Override
    public void updateService(Service service, AbstractSelector selector) throws NacosException {

    }

    @Override
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException {
        return grpcClientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }

    @Override
    public ServiceInfo subscribe(String serviceName, String groupName, String clusters) throws NacosException {
        //获取添加分组的服务名称
        String serviceNameWithGroup = NamingUtils.getGroupedName(serviceName, groupName);
        //生成服务key
        String serviceKey = ServiceInfo.getKey(serviceNameWithGroup, clusters);
        //通过key从缓存中获取服务信息
        ServiceInfo result = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
        if (null == result) {
            //如果缓存中无相关服务信息，就通过grpc发起远程调用到nacos-server中获取服务信息
            result = grpcClientProxy.subscribe(serviceName, groupName, clusters);
        }
        //添加定时任务调度，定期更新缓存信息 定时从服务器端拉取数据
        serviceInfoUpdateService.scheduleUpdateIfAbsent(serviceName, groupName, clusters);
        //把获取的缓存信息添加到本地缓存
        serviceInfoHolder.processServiceInfo(result);
        return result;
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, String clusters) throws NacosException {
        serviceInfoUpdateService.stopUpdateIfContain(serviceName, groupName, clusters);
        grpcClientProxy.unsubscribe(serviceName, groupName, clusters);
    }

    @Override
    public void updateBeatInfo(Set<Instance> modifiedInstances) {
        httpClientProxy.updateBeatInfo(modifiedInstances);
    }

    @Override
    public boolean serverHealthy() {
        return grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy();
    }

    private NamingClientProxy getExecuteClientProxy(Instance instance) {
        return instance.isEphemeral() ? grpcClientProxy : httpClientProxy;
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        serviceInfoUpdateService.shutdown();
        serverListManager.shutdown();
        httpClientProxy.shutdown();
        grpcClientProxy.shutdown();
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
