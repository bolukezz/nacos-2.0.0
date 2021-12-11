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

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingResponseCode;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.InstanceOperationContext;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.alibaba.nacos.naming.push.v1.ClientInfo;
import com.alibaba.nacos.naming.push.v1.DataSource;
import com.alibaba.nacos.naming.push.v1.NamingSubscriberServiceV1Impl;
import com.alibaba.nacos.naming.push.v1.PushClient;
import com.alibaba.nacos.naming.utils.InstanceUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.nacos.naming.misc.UtilsAndCommons.EPHEMERAL;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.PERSIST;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_REMOVE;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_UPDATE;

/**
 * Implementation of {@link InstanceOperator} by service for v1.x.
 *
 * @author xiweng.yy
 */
@Component
public class InstanceOperatorServiceImpl implements InstanceOperator {

    private final ServiceManager serviceManager;

    private final SwitchDomain switchDomain;

    private final UdpPushService pushService;

    private final NamingSubscriberServiceV1Impl subscriberServiceV1;

    private DataSource pushDataSource = new DataSource() {

        @Override
        public String getData(PushClient client) {
            ServiceInfo result = new ServiceInfo(client.getServiceName(), client.getClusters());
            try {
                Subscriber subscriber = new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(),
                    client.getIp(), client.getNamespaceId(), client.getServiceName(), client.getPort(),
                    client.getClusters());
                result = listInstance(client.getNamespaceId(), client.getServiceName(), subscriber,
                    client.getClusters(), false);
            } catch (Exception e) {
                Loggers.SRV_LOG.warn("PUSH-SERVICE: service is not modified", e);
            }

            // overdrive the cache millis to push mode
            result.setCacheMillis(switchDomain.getPushCacheMillis(client.getServiceName()));
            return JacksonUtils.toJson(result);
        }
    };

    public InstanceOperatorServiceImpl(ServiceManager serviceManager, SwitchDomain switchDomain,
                                       UdpPushService pushService, NamingSubscriberServiceV1Impl subscriberServiceV1) {
        this.serviceManager = serviceManager;
        this.switchDomain = switchDomain;
        this.pushService = pushService;
        this.subscriberServiceV1 = subscriberServiceV1;
    }

    @Override
    public void registerInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {
        com.alibaba.nacos.naming.core.Instance coreInstance = (com.alibaba.nacos.naming.core.Instance) instance;
        //注册服务端实例
        serviceManager.registerInstance(namespaceId, serviceName, coreInstance);
    }

    @Override
    public void removeInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {
        com.alibaba.nacos.naming.core.Instance coreInstance = (com.alibaba.nacos.naming.core.Instance) instance;
        //获取service，校验一下是否为空
        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", serviceName);
            return;
        }
        //移除instance
        serviceManager.removeInstance(namespaceId, serviceName, instance.isEphemeral(), coreInstance);
    }

    @Override
    public void updateInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {
        com.alibaba.nacos.naming.core.Instance coreInstance = (com.alibaba.nacos.naming.core.Instance) instance;
        serviceManager.updateInstance(namespaceId, serviceName, coreInstance);
    }

    @Override
    public void patchInstance(String namespaceId, String serviceName, InstancePatchObject patchObject)
        throws NacosException {
        com.alibaba.nacos.naming.core.Instance instance = serviceManager
            .getInstance(namespaceId, serviceName, patchObject.getCluster(), patchObject.getIp(),
                patchObject.getPort());
        if (instance == null) {
            throw new NacosException(NacosException.INVALID_PARAM, "instance not found");
        }
        if (null != patchObject.getMetadata()) {
            instance.setMetadata(patchObject.getMetadata());
        }
        if (null != patchObject.getApp()) {
            instance.setApp(patchObject.getApp());
        }
        if (null != patchObject.getEnabled()) {
            instance.setEnabled(patchObject.getEnabled());
        }
        if (null != patchObject.getHealthy()) {
            instance.setHealthy(patchObject.getHealthy());
        }
        if (null != patchObject.getApp()) {
            instance.setApp(patchObject.getApp());
        }
        instance.setLastBeat(System.currentTimeMillis());
        instance.validate();
        serviceManager.updateInstance(namespaceId, serviceName, instance);
    }

    /**
     * 服务端返回所有的列表，那么就绪看客户端发起订阅通知的地方，getServiceInfo()
     */
    @Override
    public ServiceInfo listInstance(String namespaceId, String serviceName, Subscriber subscriber, String cluster,
                                    boolean healthOnly) throws Exception {
        //这一部分代码，其实就干了2件事，1是根据namespace与serviceName 到ServiceManager下面的serviceMap获取对应的service对象。
        //2是判断upd端口与这个客户端版本啥的，看看适不适合推送，我们这里udp端口是0 ，很显然不适合。
        ClientInfo clientInfo = new ClientInfo(subscriber.getAgent());
        String clientIP = subscriber.getIp();
        ServiceInfo result = new ServiceInfo(serviceName, cluster);
        //获取service
        Service service = serviceManager.getService(namespaceId, serviceName);
        //默认的cache时间
        long cacheMillis = switchDomain.getDefaultCacheMillis();

        // now try to enable the push
        try {
            //就是判断客户端的语言，然后版本一些东西，看看能不能推送，如果可以的话，就可以添加这个客户端
            if (subscriber.getPort() > 0 && pushService.canEnablePush(subscriber.getAgent())) {
                //添加客户端
                subscriberServiceV1.addClient(namespaceId, serviceName, cluster, subscriber.getAgent(),
                    new InetSocketAddress(clientIP, subscriber.getPort()), pushDataSource, StringUtils.EMPTY,
                    StringUtils.EMPTY);
                cacheMillis = switchDomain.getPushCacheMillis(serviceName);
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.error("[NACOS-API] failed to added push client {}, {}:{}", clientInfo, clientIP,
                subscriber.getPort(), e);
            cacheMillis = switchDomain.getDefaultCacheMillis();
        }

        //就是service是null的时候，是null的时候组装响应参数返回给客户端了，我们看下这段代码，先是检查service 可不可用
        // ，接着就是根据cluster 获取这个service 下面的instance列表了，根据cluster获取这个服务下面对应的实例集合。
        //接着就是获取这个selector ，使用selector 来过滤实例集合。
        if (service == null) {
            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("no instance to serve for service: {}", serviceName);
            }
            result.setCacheMillis(cacheMillis);
            return result;
        }
        //检查服务是否可用
        checkIfDisabled(service);
        //从service中找这些cluster的instance
        List<com.alibaba.nacos.naming.core.Instance> srvedIPs = service
            .srvIPs(Arrays.asList(StringUtils.split(cluster, ",")));

        // filter ips using selector:
        //使用选择器过滤
        if (service.getSelector() != null && StringUtils.isNotBlank(clientIP)) {
            srvedIPs = service.getSelector().select(clientIP, srvedIPs);
        }

        if (CollectionUtils.isEmpty(srvedIPs)) {

            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("no instance to serve for service: {}", serviceName);
            }

            result.setCacheMillis(cacheMillis);
            result.setLastRefTime(System.currentTimeMillis());
            result.setChecksum(service.getChecksum());
            return result;
        }
        //接着就是 创建一个map，然后用来区分健康的实例与不健康的实例，健康的实例key就是 Boolean.TRUE 不健康的就是Boolean.FALSE
        // ，遍历区分开，接着就是判断是否检查，默认是false的，获取服务保护的阈值，默认是0 ， 如果健康的服务实例数量占比小于这个阈值的话，他就会将不健康的实例也放到健康的里面，
        // 这就是nacos的服务保护机制，可以联想下eureka的服务保护机制，为啥要把不健康的放到健康里面，因为后面返回给客户端的时候，只返回健康的这个key下面，
        // 这样就达到了活着实例数量低于某个阈值后不进行服务摘除的效果。
        //后面就是遍历那个map只要Boolean.TRUE 与可用的服务，然后塞到hosts 这个map中
        //最后就是封装返回结果，然后返回给客户端。

        long total = 0;
        Map<Boolean, List<com.alibaba.nacos.naming.core.Instance>> ipMap = new HashMap<>(2);
        ipMap.put(Boolean.TRUE, new ArrayList<>());
        ipMap.put(Boolean.FALSE, new ArrayList<>());

        //遍历，将健康的与不健康的实例区分开
        for (com.alibaba.nacos.naming.core.Instance ip : srvedIPs) {
            // remove disabled instance:
            if (!ip.isEnabled()) {
                continue;
            }
            ipMap.get(ip.isHealthy()).add(ip);
            total += 1;
        }

        //保护阈值
        double threshold = service.getProtectThreshold();
        List<Instance> hosts;
        //活着的instance比例小于保护阈值的话
        if ((float) ipMap.get(Boolean.TRUE).size() / total <= threshold) {

            Loggers.SRV_LOG.warn("protect threshold reached, return all ips, service: {}", result.getName());
            result.setReachProtectionThreshold(true);
            //将所有都设置为健康以保护，就把所有的不健康的实例添加到健康里面
            hosts = Stream.of(Boolean.TRUE, Boolean.FALSE).map(ipMap::get).flatMap(Collection::stream)
                .map(InstanceUtil::deepCopy)
                // set all to `healthy` state to protect
                .peek(instance -> instance.setHealthy(true)).collect(Collectors.toCollection(LinkedList::new));
        } else {
            result.setReachProtectionThreshold(false);
            hosts = new LinkedList<>(ipMap.get(Boolean.TRUE));
            if (!healthOnly) {
                hosts.addAll(ipMap.get(Boolean.FALSE));
            }
        }

        result.setHosts(hosts);
        result.setCacheMillis(cacheMillis);
        result.setLastRefTime(System.currentTimeMillis());
        result.setChecksum(service.getChecksum());
        return result;
    }

    @Override
    public Instance getInstance(String namespaceId, String serviceName, String cluster, String ip, int port)
        throws NacosException {
        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "no service " + serviceName + " found!");
        }
        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);
        List<com.alibaba.nacos.naming.core.Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            throw new NacosException(NacosException.NOT_FOUND,
                "no ips found for cluster " + cluster + " in service " + serviceName);
        }
        for (com.alibaba.nacos.naming.core.Instance each : ips) {
            if (each.getIp().equals(ip) && each.getPort() == port) {
                return each;
            }
        }
        throw new NacosException(NacosException.NOT_FOUND, "no matched ip found!");
    }

    private void checkIfDisabled(Service service) throws Exception {
        if (!service.getEnabled()) {
            throw new Exception("service is disabled now.");
        }
    }

    /**
     * 先是根据namespaceId, serviceName, clusterName, ip, port 这个参数调用 ServiceManager的getInstance 获取对应的instance，
     * 其实就是先根据namespace从serviceMap中获取对应的service，接着根据cluster从service的clusterMap中获取对应cluster的instance集合，然后再遍历比对ip与port。
     * 如果没有找到对应的instance，而且beatInfo不是null，就会进行服务注册。
     * 接着就是根据namespace与serviceName获取service，然后调用service的processClientBeat 方法处理心跳。
     */
    @Override
    public int handleBeat(String namespaceId, String serviceName, String ip, int port, String cluster,
                          RsInfo clientBeat) throws NacosException {
        //获取到对应的instance
        com.alibaba.nacos.naming.core.Instance instance = serviceManager
            .getInstance(namespaceId, serviceName, cluster, ip, port);

        //如果不存在对应的实例，那么就进行实例化
        if (instance == null) {
            if (clientBeat == null) {
                return NamingResponseCode.RESOURCE_NOT_FOUND;
            }

            Loggers.SRV_LOG.warn("[CLIENT-BEAT] The instance has been removed for health mechanism, "
                + "perform data compensation operations, beat: {}, serviceName: {}", clientBeat, serviceName);

            instance = new com.alibaba.nacos.naming.core.Instance();
            instance.setPort(clientBeat.getPort());
            instance.setIp(clientBeat.getIp());
            instance.setWeight(clientBeat.getWeight());
            instance.setMetadata(clientBeat.getMetadata());
            instance.setClusterName(cluster);
            instance.setServiceName(serviceName);
            instance.setInstanceId(instance.getInstanceId());
            instance.setEphemeral(clientBeat.isEphemeral());
            //注册
            serviceManager.registerInstance(namespaceId, serviceName, instance);
        }

        //获取到对应的服务
        Service service = serviceManager.getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.SERVER_ERROR,
                "service not found: " + serviceName + "@" + namespaceId);
        }
        if (clientBeat == null) {
            clientBeat = new RsInfo();
            clientBeat.setIp(ip);
            clientBeat.setPort(port);
            clientBeat.setCluster(cluster);
        }
        //处理心跳
        service.processClientBeat(clientBeat);
        return NamingResponseCode.OK;
    }

    @Override
    public long getHeartBeatInterval(String namespaceId, String serviceName, String ip, int port, String cluster) {
        com.alibaba.nacos.naming.core.Instance instance = serviceManager
            .getInstance(namespaceId, serviceName, cluster, ip, port);
        if (null != instance && instance.containsMetadata(PreservedMetadataKeys.HEART_BEAT_INTERVAL)) {
            return instance.getInstanceHeartBeatInterval();
        }
        return switchDomain.getClientBeatInterval();
    }

    @Override
    public List<? extends Instance> listAllInstances(String namespaceId, String serviceName) throws NacosException {
        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "service: " + serviceName + " not found.");
        }
        return service.allIPs();
    }

    @Override
    public List<String> batchUpdateMetadata(String namespaceId, InstanceOperationInfo instanceOperationInfo,
                                            Map<String, String> metadata) throws NacosException {
        return batchOperate(namespaceId, instanceOperationInfo, metadata, UPDATE_INSTANCE_METADATA_ACTION_UPDATE);
    }

    @Override
    public List<String> batchDeleteMetadata(String namespaceId, InstanceOperationInfo instanceOperationInfo,
                                            Map<String, String> metadata) throws NacosException {
        return batchOperate(namespaceId, instanceOperationInfo, metadata, UPDATE_INSTANCE_METADATA_ACTION_REMOVE);
    }

    private List<String> batchOperate(String namespaceId, InstanceOperationInfo instanceOperationInfo,
                                      Map<String, String> metadata, String updateInstanceMetadataAction) {
        List<String> result = new LinkedList<>();
        for (com.alibaba.nacos.naming.core.Instance each : batchOperateMetadata(namespaceId, instanceOperationInfo,
            metadata, updateInstanceMetadataAction)) {
            result.add(each.getDatumKey() + ":" + (each.isEphemeral() ? EPHEMERAL : PERSIST));
        }
        return result;
    }

    private List<com.alibaba.nacos.naming.core.Instance> batchOperateMetadata(String namespace,
                                                                              InstanceOperationInfo instanceOperationInfo, Map<String, String> metadata, String action) {
        Function<InstanceOperationContext, List<com.alibaba.nacos.naming.core.Instance>> operateFunction =
            instanceOperationContext -> {
                try {
                    return serviceManager.updateMetadata(instanceOperationContext.getNamespace(),
                        instanceOperationContext.getServiceName(), instanceOperationContext.getEphemeral(), action,
                        instanceOperationContext.getAll(), instanceOperationContext.getInstances(), metadata);
                } catch (NacosException e) {
                    Loggers.SRV_LOG.warn("UPDATE-METADATA: updateMetadata failed", e);
                }
                return new ArrayList<>();
            };
        return serviceManager.batchOperate(namespace, instanceOperationInfo, operateFunction);
    }
}
