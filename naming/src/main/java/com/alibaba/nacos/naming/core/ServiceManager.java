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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.Objects;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeer;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeerSet;
import com.alibaba.nacos.naming.core.v2.cleaner.EmptyServiceAutoCleaner;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.Message;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.ServiceStatusSynchronizer;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.Synchronizer;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.InstanceOperationContext;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_REMOVE;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_UPDATE;

/**
 * Core manager storing all services in Nacos.
 *
 * @author nkorange
 */
@Component
public class ServiceManager implements RecordListener<Service> {

    /**
     * Map(namespace, Map(group::serviceName, Service)).
     */
    // namespace   ---》 serivceName ，service
    private final Map<String, Map<String, Service>> serviceMap = new ConcurrentHashMap<>();

    private final LinkedBlockingDeque<ServiceKey> toBeUpdatedServicesQueue = new LinkedBlockingDeque<>(1024 * 1024);

    private final Synchronizer synchronizer = new ServiceStatusSynchronizer();

    private final Lock lock = new ReentrantLock();

    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    private final SwitchDomain switchDomain;

    private final DistroMapper distroMapper;

    private final ServerMemberManager memberManager;

    private final UdpPushService pushService;

    private final RaftPeerSet raftPeerSet;

    private final Object putServiceLock = new Object();

    @Value("${nacos.naming.empty-service.auto-clean:false}")
    private boolean emptyServiceAutoClean;

    @Value("${nacos.naming.empty-service.clean.initial-delay-ms:60000}")
    private int cleanEmptyServiceDelay;

    @Value("${nacos.naming.empty-service.clean.period-time-ms:20000}")
    private int cleanEmptyServicePeriod;

    public ServiceManager(SwitchDomain switchDomain, DistroMapper distroMapper, ServerMemberManager memberManager,
                          UdpPushService pushService, RaftPeerSet raftPeerSet) {
        this.switchDomain = switchDomain;
        this.distroMapper = distroMapper;
        this.memberManager = memberManager;
        this.pushService = pushService;
        this.raftPeerSet = raftPeerSet;
    }

    /**
     * Init service maneger.
     */
    @PostConstruct
    public void init() {
        GlobalExecutor.scheduleServiceReporter(new ServiceReporter(), 60000, TimeUnit.MILLISECONDS);

        GlobalExecutor.submitServiceUpdateManager(new UpdatedServiceProcessor());

        if (emptyServiceAutoClean) {

            Loggers.SRV_LOG.info("open empty service auto clean job, initialDelay : {} ms, period : {} ms",
                cleanEmptyServiceDelay, cleanEmptyServicePeriod);

            // delay 60s, period 20s;

            // This task is not recommended to be performed frequently in order to avoid
            // the possibility that the service cache information may just be deleted
            // and then created due to the heartbeat mechanism

            GlobalExecutor
                .scheduleServiceAutoClean(new EmptyServiceAutoCleaner(this, distroMapper), cleanEmptyServiceDelay,
                    cleanEmptyServicePeriod);
        }

        try {
            Loggers.SRV_LOG.info("listen for service meta change");
            consistencyService.listen(KeyBuilder.SERVICE_META_KEY_PREFIX, this);
        } catch (NacosException e) {
            Loggers.SRV_LOG.error("listen for service meta change failed!");
        }
    }

    public Map<String, Service> chooseServiceMap(String namespaceId) {
        return serviceMap.get(namespaceId);
    }

    /**
     * Add a service into queue to update.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param serverIP    target server ip
     * @param checksum    checksum of service
     */
    public void addUpdatedServiceToQueue(String namespaceId, String serviceName, String serverIP, String checksum) {
        lock.lock();
        try {
            toBeUpdatedServicesQueue
                .offer(new ServiceKey(namespaceId, serviceName, serverIP, checksum), 5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            toBeUpdatedServicesQueue.poll();
            toBeUpdatedServicesQueue.add(new ServiceKey(namespaceId, serviceName, serverIP, checksum));
            Loggers.SRV_LOG.error("[DOMAIN-STATUS] Failed to add service to be updated to queue.", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean interests(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public boolean matchUnlistenKey(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public void onChange(String key, Service service) throws Exception {
        try {
            if (service == null) {
                Loggers.SRV_LOG.warn("received empty push from raft, key: {}", key);
                return;
            }

            if (StringUtils.isBlank(service.getNamespaceId())) {
                service.setNamespaceId(Constants.DEFAULT_NAMESPACE_ID);
            }

            Loggers.RAFT.info("[RAFT-NOTIFIER] datum is changed, key: {}, value: {}", key, service);

            Service oldDom = getService(service.getNamespaceId(), service.getName());

            if (oldDom != null) {
                oldDom.update(service);
                // re-listen to handle the situation when the underlying listener is removed:
                consistencyService
                    .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true),
                        oldDom);
                consistencyService
                    .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false),
                        oldDom);
            } else {
                putServiceAndInit(service);
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[NACOS-SERVICE] error while processing service update", e);
        }
    }

    @Override
    public void onDelete(String key) throws Exception {
        String namespace = KeyBuilder.getNamespace(key);
        String name = KeyBuilder.getServiceName(key);
        Service service = chooseServiceMap(namespace).get(name);
        Loggers.RAFT.info("[RAFT-NOTIFIER] datum is deleted, key: {}", key);

        if (service != null) {
            service.destroy();
            String ephemeralInstanceListKey = KeyBuilder.buildInstanceListKey(namespace, name, true);
            String persistInstanceListKey = KeyBuilder.buildInstanceListKey(namespace, name, false);
            consistencyService.remove(ephemeralInstanceListKey);
            consistencyService.remove(persistInstanceListKey);

            // remove listeners of key to avoid mem leak
            consistencyService.unListen(ephemeralInstanceListKey, service);
            consistencyService.unListen(persistInstanceListKey, service);
            consistencyService.unListen(KeyBuilder.buildServiceMetaKey(namespace, name), service);
            Loggers.SRV_LOG.info("[DEAD-SERVICE] {}", service.toJson());
        }

        chooseServiceMap(namespace).remove(name);
    }

    private class UpdatedServiceProcessor implements Runnable {

        //get changed service from other server asynchronously
        @Override
        public void run() {
            ServiceKey serviceKey = null;

            try {
                while (true) {
                    try {
                        serviceKey = toBeUpdatedServicesQueue.take();
                    } catch (Exception e) {
                        Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while taking item from LinkedBlockingDeque.");
                    }

                    if (serviceKey == null) {
                        continue;
                    }
                    GlobalExecutor.submitServiceUpdate(new ServiceUpdater(serviceKey));
                }
            } catch (Exception e) {
                Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while update service: {}", serviceKey, e);
            }
        }
    }


    private class ServiceUpdater implements Runnable {

        String namespaceId;

        String serviceName;

        String serverIP;

        public ServiceUpdater(ServiceKey serviceKey) {
            this.namespaceId = serviceKey.getNamespaceId();
            this.serviceName = serviceKey.getServiceName();
            this.serverIP = serviceKey.getServerIP();
        }

        @Override
        public void run() {
            try {
                updatedHealthStatus(namespaceId, serviceName, serverIP);
            } catch (Exception e) {
                Loggers.SRV_LOG
                    .warn("[DOMAIN-UPDATER] Exception while update service: {} from {}, error: {}", serviceName,
                        serverIP, e);
            }
        }
    }

    public RaftPeer getMySelfClusterState() {
        return raftPeerSet.local();
    }

    /**
     * Update health status of instance in service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param serverIP    source server Ip
     */
    public void updatedHealthStatus(String namespaceId, String serviceName, String serverIP) {
        Message msg = synchronizer.get(serverIP, UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
        JsonNode serviceJson = JacksonUtils.toObj(msg.getData());

        ArrayNode ipList = (ArrayNode) serviceJson.get("ips");
        Map<String, String> ipsMap = new HashMap<>(ipList.size());
        for (int i = 0; i < ipList.size(); i++) {

            String ip = ipList.get(i).asText();
            String[] strings = ip.split("_");
            ipsMap.put(strings[0], strings[1]);
        }

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            return;
        }

        boolean changed = false;

        List<Instance> instances = service.allIPs();
        for (Instance instance : instances) {

            boolean valid = Boolean.parseBoolean(ipsMap.get(instance.toIpAddr()));
            if (valid != instance.isHealthy()) {
                changed = true;
                instance.setHealthy(valid);
                Loggers.EVT_LOG.info("{} {SYNC} IP-{} : {}:{}@{}", serviceName,
                    (instance.isHealthy() ? "ENABLED" : "DISABLED"), instance.getIp(), instance.getPort(),
                    instance.getClusterName());
            }
        }

        if (changed) {
            pushService.serviceChanged(service);
            if (Loggers.EVT_LOG.isDebugEnabled()) {
                StringBuilder stringBuilder = new StringBuilder();
                List<Instance> allIps = service.allIPs();
                for (Instance instance : allIps) {
                    stringBuilder.append(instance.toIpAddr()).append("_").append(instance.isHealthy()).append(",");
                }
                Loggers.EVT_LOG
                    .debug("[HEALTH-STATUS-UPDATED] namespace: {}, service: {}, ips: {}", service.getNamespaceId(),
                        service.getName(), stringBuilder.toString());
            }
        }

    }

    public Set<String> getAllServiceNames(String namespaceId) {
        return serviceMap.get(namespaceId).keySet();
    }

    public Map<String, Set<String>> getAllServiceNames() {

        Map<String, Set<String>> namesMap = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            namesMap.put(namespaceId, serviceMap.get(namespaceId).keySet());
        }
        return namesMap;
    }

    public Set<String> getAllNamespaces() {
        return serviceMap.keySet();
    }

    public List<String> getAllServiceNameList(String namespaceId) {
        if (chooseServiceMap(namespaceId) == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(chooseServiceMap(namespaceId).keySet());
    }

    public Map<String, Set<Service>> getResponsibleServices() {
        Map<String, Set<Service>> result = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            result.put(namespaceId, new HashSet<>());
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                Service service = entry.getValue();
                if (distroMapper.responsible(entry.getKey())) {
                    result.get(namespaceId).add(service);
                }
            }
        }
        return result;
    }

    public int getResponsibleServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                if (distroMapper.responsible(entry.getKey())) {
                    serviceCount++;
                }
            }
        }
        return serviceCount;
    }

    public int getResponsibleInstanceCount() {
        Map<String, Set<Service>> responsibleServices = getResponsibleServices();
        int count = 0;
        for (String namespaceId : responsibleServices.keySet()) {
            for (Service service : responsibleServices.get(namespaceId)) {
                count += service.allIPs().size();
            }
        }

        return count;
    }

    /**
     * Fast remove service.
     *
     * <p>Remove service bu async.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @throws NacosException exception
     */
    public void easyRemoveService(String namespaceId, String serviceName) throws NacosException {

        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "specified service not exist, serviceName : " + serviceName);
        }

        consistencyService.remove(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName));
    }

    public void addOrReplaceService(Service service) throws NacosException {
        consistencyService.put(KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName()), service);
    }

    public void createEmptyService(String namespaceId, String serviceName, boolean local) throws NacosException {
        //创建如果不存在的话
        createServiceIfAbsent(namespaceId, serviceName, local, null);
    }

    /**
     * Create service if not exist.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param local       whether create service by local
     * @param cluster     cluster
     * @throws NacosException nacos exception
     */
    //这里就是创建一个空的service
    public void createServiceIfAbsent(String namespaceId, String serviceName, boolean local, Cluster cluster)
        throws NacosException {
        //根据namespace和serviceName获取service
        Service service = getService(namespaceId, serviceName);
        if (service == null) {//如果不存在的话
            Loggers.SRV_LOG.info("creating empty service {}:{}", namespaceId, serviceName);
            service = new Service();
            service.setName(serviceName);
            service.setNamespaceId(namespaceId);
            //取出来group
            service.setGroupName(NamingUtils.getGroupName(serviceName));
            // now validate the service. if failed, exception will be thrown
            //最后修改事件时间
            service.setLastModifiedMillis(System.currentTimeMillis());
            service.recalculateChecksum();
            if (cluster != null) { //集群
                cluster.setService(service);
                service.getClusterMap().put(cluster.getName(), cluster);
            }
            service.validate();
            //put service并且初始化
            putServiceAndInit(service);
            if (!local) {
                addOrReplaceService(service);
            }
        }
    }

    /**
     * Register an instance to a service in AP mode.
     *
     * <p>This method creates service or cluster silently if they don't exist.
     *
     * @param namespaceId id of namespace
     * @param serviceName service name
     * @param instance    instance to register
     * @throws Exception any error occurred in the process
     */
    public void registerInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {
        //创建空的service
        createEmptyService(namespaceId, serviceName, instance.isEphemeral());
        //先是根据namespace与serviceName 获取service ，如果没有的话，就创建，最开始的时候，肯定是没有的，然后就会创建一个service，看下这个getService 方法
        //获取service
        Service service = getService(namespaceId, serviceName);
        //这里指定不能是null了
        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }
        //添加实例
        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    /**
     * Update instance to service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param instance    instance
     * @throws NacosException nacos exception
     */
    public void updateInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        if (!service.allIPs().contains(instance)) {
            throw new NacosException(NacosException.INVALID_PARAM, "instance not exist: " + instance);
        }

        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    /**
     * Update instance's metadata.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param action      update or remove
     * @param ips         need update instances
     * @param metadata    target metadata
     * @return update succeed instances
     * @throws NacosException nacos exception
     */
    public List<Instance> updateMetadata(String namespaceId, String serviceName, boolean isEphemeral, String action,
                                         boolean all, List<Instance> ips, Map<String, String> metadata) throws NacosException {

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        List<Instance> locatedInstance = getLocatedInstance(namespaceId, serviceName, isEphemeral, all, ips);

        if (CollectionUtils.isEmpty(locatedInstance)) {
            throw new NacosException(NacosException.INVALID_PARAM, "not locate instances, input instances: " + ips);
        }

        if (UPDATE_INSTANCE_METADATA_ACTION_UPDATE.equals(action)) {
            locatedInstance.forEach(ele -> ele.getMetadata().putAll(metadata));
        } else if (UPDATE_INSTANCE_METADATA_ACTION_REMOVE.equals(action)) {
            Set<String> removeKeys = metadata.keySet();
            for (String removeKey : removeKeys) {
                locatedInstance.forEach(ele -> ele.getMetadata().remove(removeKey));
            }
        }
        Instance[] instances = new Instance[locatedInstance.size()];
        locatedInstance.toArray(instances);

        addInstance(namespaceId, serviceName, isEphemeral, instances);

        return locatedInstance;
    }

    /**
     * Locate consistency's datum by all or instances provided.
     *
     * @param namespaceId        namespace
     * @param serviceName        serviceName
     * @param isEphemeral        isEphemeral
     * @param all                get from consistencyService directly
     * @param waitLocateInstance instances provided
     * @return located instances
     * @throws NacosException nacos exception
     */
    public List<Instance> getLocatedInstance(String namespaceId, String serviceName, boolean isEphemeral, boolean all,
                                             List<Instance> waitLocateInstance) throws NacosException {
        List<Instance> locatedInstance;

        //need the newest data from consistencyService
        Datum datum = consistencyService.get(KeyBuilder.buildInstanceListKey(namespaceId, serviceName, isEphemeral));
        if (datum == null) {
            throw new NacosException(NacosException.NOT_FOUND,
                "instances from consistencyService not exist, namespace: " + namespaceId + ", service: "
                    + serviceName + ", ephemeral: " + isEphemeral);
        }

        if (all) {
            locatedInstance = ((Instances) datum.value).getInstanceList();
        } else {
            locatedInstance = new ArrayList<>();
            for (Instance instance : waitLocateInstance) {
                Instance located = locateInstance(((Instances) datum.value).getInstanceList(), instance);
                if (located == null) {
                    continue;
                }
                locatedInstance.add(located);
            }
        }

        return locatedInstance;
    }

    private Instance locateInstance(List<Instance> sources, Instance target) {
        if (CollectionUtils.isEmpty(sources)) {
            return null;
        }

        for (Instance element : sources) {
            //also need clusterName equals, the same instance maybe exist in two cluster.
            if (Objects.equals(element, target) && Objects.equals(element.getClusterName(), target.getClusterName())) {
                return element;
            }
        }
        return null;
    }

    /**
     * Add instance to service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param ephemeral   whether instance is ephemeral
     * @param ips         instances
     * @throws NacosException nacos exception
     */
    /**
     * 首先是生成一个key，这个key是根据你namespace，serviceName ，是否临时节点来决定的，我们这里是临时的，直接看下生成的结果就可以了
     * com.alibaba.nacos.naming.iplist.ephemeral.{namespace}##{serviceName}
     * 接着就是获取service， 上锁，调用addIpAddresses 得到一个instance集合。
     * UPDATE_INSTANCE_ACTION_ADD是add，再往下这个方法太长我们就不看了，主要就是新的instance 与之前的instance进行合并啥的，生成一个新的instance集合。
     * 接着就是创建一个instances 对象，将instance集合塞进去，最后低哦啊用consiitencyService 组件进行保存。
     */
    public void addInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips)
        throws NacosException {
        //创建一个key
        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);
        //获取service
        Service service = getService(namespaceId, serviceName);

        synchronized (service) {
            //更新，然后获得到这个服务里面所有实例
            List<Instance> instanceList = addIpAddresses(service, ephemeral, ips);
            //塞到instances中
            Instances instances = new Instances();
            instances.setInstanceList(instanceList);
            //调用保存
            //调用一致性服务的put方法，这里会调用到DelegateConsistencyServiceImpl#put方法
            consistencyService.put(key, instances);
        }
    }

    /**
     * Remove instance from service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param ephemeral   whether instance is ephemeral
     * @param ips         instances
     * @throws NacosException nacos exception
     */
    public void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips)
        throws NacosException {
        //获取service
        Service service = getService(namespaceId, serviceName);
       //加锁
        synchronized (service) {
            //移除instance
            removeInstance(namespaceId, serviceName, ephemeral, service, ips);
        }
    }

    /**
     * 先生成一个服务列表的key这个key与你instance是否是临时节点有关系，如果是临时节点，生成的key是这个样子的com.alibaba.nacos.naming.iplist.ephemeral.{namespace}##{serviceName}
     * 永久节点就是com.alibaba.nacos.naming.iplist.{namespace}##{serviceName} 这个样子。
     * 接着就是调用substractIpAddresses 方法用之前的instance列表减去 这次要下线的实例列表，然后生成一份新的剖去下线的实例列表。

     */
    private void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Service service,
                                Instance... ips) throws NacosException {

        //生成一个key
        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);

        //返回下线完剩下的instance集合
        List<Instance> instanceList = substractIpAddresses(service, ephemeral, ips);

        //封装instances
        Instances instances = new Instances();
        instances.setInstanceList(instanceList);

        //交给一致性服务进行存储，通知等
        consistencyService.put(key, instances);
    }

    public Instance getInstance(String namespaceId, String serviceName, String cluster, String ip, int port) {
        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            return null;
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            return null;
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                return instance;
            }
        }

        return null;
    }

    /**
     * batch operate kinds of resources.
     *
     * @param namespace       namespace.
     * @param operationInfo   operation resources description.
     * @param operateFunction some operation defined by kinds of situation.
     */
    public List<Instance> batchOperate(String namespace, InstanceOperationInfo operationInfo,
                                       Function<InstanceOperationContext, List<Instance>> operateFunction) {
        List<Instance> operatedInstances = new ArrayList<>();
        try {
            String serviceName = operationInfo.getServiceName();
            NamingUtils.checkServiceNameFormat(serviceName);
            // type: ephemeral/persist
            InstanceOperationContext operationContext;
            String type = operationInfo.getConsistencyType();
            if (!StringUtils.isEmpty(type)) {
                switch (type) {
                    case UtilsAndCommons.EPHEMERAL:
                        operationContext = new InstanceOperationContext(namespace, serviceName, true, true);
                        operatedInstances.addAll(operateFunction.apply(operationContext));
                        break;
                    case UtilsAndCommons.PERSIST:
                        operationContext = new InstanceOperationContext(namespace, serviceName, false, true);
                        operatedInstances.addAll(operateFunction.apply(operationContext));
                        break;
                    default:
                        Loggers.SRV_LOG
                            .warn(
                                "UPDATE-METADATA: services.all value is illegal, it should be ephemeral/persist. ignore the service '"
                                    + serviceName + "'");
                        break;
                }
            } else {
                List<Instance> instances = (List<Instance>) operationInfo.getInstances();
                if (!CollectionUtils.isEmpty(instances)) {
                    //ephemeral:instances or persist:instances
                    Map<Boolean, List<Instance>> instanceMap = instances.stream()
                        .collect(Collectors.groupingBy(ele -> ele.isEphemeral()));

                    for (Map.Entry<Boolean, List<Instance>> entry : instanceMap.entrySet()) {
                        operationContext = new InstanceOperationContext(namespace, serviceName, entry.getKey(), false,
                            entry.getValue());
                        operatedInstances.addAll(operateFunction.apply(operationContext));
                    }
                }
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("UPDATE-METADATA: update metadata failed, ignore the service '" + operationInfo
                .getServiceName() + "'", e);
        }
        return operatedInstances;
    }

    /**
     * Compare and get new instance list.
     *
     * @param service   service
     * @param action    {@link UtilsAndCommons#UPDATE_INSTANCE_ACTION_REMOVE} or {@link UtilsAndCommons#UPDATE_INSTANCE_ACTION_ADD}
     * @param ephemeral whether instance is ephemeral
     * @param ips       instances
     * @return instance list after operation
     * @throws NacosException nacos exception
     */
    /**
     *其实就是将之前的instance弄出来，然后放到这个instanceMap中，然后遍历这个要删除的instance集合，如果是删除action的话，就从instanceMap中移除这个DatumKey，这个key就是这个样子
     * ip:port:unknown:{cluster}。到最后这个instanceMap就剩下抛去我们要下线的instance了。
     * 接着removeInstance 这个方法往下看，就是创建一个instances对象，然后将instance集合塞到这里面。接着就是交给consistencyService组件来进行put操作。
     * 这里其实就是调用了EphemeralConsistencyService的实现类DistroConsistencyServiceImpl 的put方法。往下的步骤我们就不赘述了，再往下就与服务注册后续的逻辑一摸一样的
     * ，就是封装instance集合与key ，封装成一个Datum，然后将这个Datum 塞到dataStore 这个存储组件中，这个组件实际就是个map。
     * 接着就是往Notifier 这个组件里面的一个task队列添加一个事件通知任务，然后就完事了（其实这里还有向其他server 同步的步骤，我们这里暂时先不研究），就可以将响应返回给客户端了
     * 。这个时候，其实service 对象里面的instance列表并没有更新，这就是所谓nacos的异步注册异步下线，会有一个后台线程，不停的从Notifier组件中的task 队列中取出task，然后调用handle
     * 方法进行事件通知，其实就通知到service 对象的onChange 方法里面了，其实更新操作都是这个方法做的。
     */
    public List<Instance> updateIpAddresses(Service service, String action, boolean ephemeral, Instance... ips)
        throws NacosException {

        Datum datum = consistencyService
            .get(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), ephemeral));

        List<Instance> currentIPs = service.allIPs(ephemeral);
        Map<String, Instance> currentInstances = new HashMap<>(currentIPs.size());
        Set<String> currentInstanceIds = Sets.newHashSet();

        for (Instance instance : currentIPs) {
            currentInstances.put(instance.toIpAddr(), instance);
            currentInstanceIds.add(instance.getInstanceId());
        }

        Map<String, Instance> instanceMap;
        if (datum != null && null != datum.value) {
            instanceMap = setValid(((Instances) datum.value).getInstanceList(), currentInstances);
        } else {
            instanceMap = new HashMap<>(ips.length);
        }

        //遍历新的，然后覆盖
        for (Instance instance : ips) {
            //如果这个服务中没有对应的cluster集群
            if (!service.getClusterMap().containsKey(instance.getClusterName())) {
                //创建一个
                Cluster cluster = new Cluster(instance.getClusterName(), service);
                //初始化
                cluster.init();
                //添加到map中
                service.getClusterMap().put(instance.getClusterName(), cluster);
                Loggers.SRV_LOG
                    .warn("cluster: {} not found, ip: {}, will create new cluster with default configuration.",
                        instance.getClusterName(), instance.toJson());
            }

            //remove操作
            if (UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE.equals(action)) {
                //就是从instanceMap中删除这个instance
                instanceMap.remove(instance.getDatumKey());
            } else {
                //生成instanceID
                Instance oldInstance = instanceMap.get(instance.getDatumKey());
                if (oldInstance != null) {
                    instance.setInstanceId(oldInstance.getInstanceId());
                } else {
                    instance.setInstanceId(instance.generateInstanceId(currentInstanceIds));
                }
                //放到instanceMap中
                instanceMap.put(instance.getDatumKey(), instance);
            }

        }

        if (instanceMap.size() <= 0 && UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD.equals(action)) {
            throw new IllegalArgumentException(
                "ip list can not be empty, service: " + service.getName() + ", ip list: " + JacksonUtils
                    .toJson(instanceMap.values()));
        }

        return new ArrayList<>(instanceMap.values());
    }

    private List<Instance> substractIpAddresses(Service service, boolean ephemeral, Instance... ips)
        throws NacosException {
        //这里传入REMOVE
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE, ephemeral, ips);
    }

    private List<Instance> addIpAddresses(Service service, boolean ephemeral, Instance... ips) throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD, ephemeral, ips);
    }

    private Map<String, Instance> setValid(List<Instance> oldInstances, Map<String, Instance> map) {

        Map<String, Instance> instanceMap = new HashMap<>(oldInstances.size());
        for (Instance instance : oldInstances) {
            Instance instance1 = map.get(instance.toIpAddr());
            if (instance1 != null) {
                instance.setHealthy(instance1.isHealthy());
                instance.setLastBeat(instance1.getLastBeat());
            }
            instanceMap.put(instance.getDatumKey(), instance);
        }
        return instanceMap;
    }

    public Service getService(String namespaceId, String serviceName) {
        if (serviceMap.get(namespaceId) == null) { //如果namespace没有的话
            return null;
        }
        //这个其实就是去serviceMap这个成员中获取
        return chooseServiceMap(namespaceId).get(serviceName);
    }

    public boolean containService(String namespaceId, String serviceName) {
        return getService(namespaceId, serviceName) != null;
    }

    /**
     * Put service into manager.
     *
     * @param service service
     */
    public void putService(Service service) {
        if (!serviceMap.containsKey(service.getNamespaceId())) {
            synchronized (putServiceLock) {
                if (!serviceMap.containsKey(service.getNamespaceId())) {
                    serviceMap.put(service.getNamespaceId(), new ConcurrentSkipListMap<>());
                }
            }
        }
        serviceMap.get(service.getNamespaceId()).putIfAbsent(service.getName(), service);
    }

    //将service缓存起来并且初始化这个service
    //就是将这个service 放到 serviceMap中，然后service进行初始化， 再就是添加两个监听器。
    private void putServiceAndInit(Service service) throws NacosException {
        //将这个service放到serviceMap中
        putService(service);
        service = getService(service.getNamespaceId(), service.getName());
        //服务初始化，重点看下这个方法
        service.init();
        consistencyService
            .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true), service);
        consistencyService
            .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false), service);
        Loggers.SRV_LOG.info("[NEW-SERVICE] {}", service.toJson());
    }

    /**
     * Search services.
     *
     * @param namespaceId namespace
     * @param regex       search regex
     * @return list of service which searched
     */
    public List<Service> searchServices(String namespaceId, String regex) {
        List<Service> result = new ArrayList<>();
        for (Map.Entry<String, Service> entry : chooseServiceMap(namespaceId).entrySet()) {
            Service service = entry.getValue();
            String key = service.getName() + ":" + ArrayUtils.toString(service.getOwners());
            if (key.matches(regex)) {
                result.add(service);
            }
        }

        return result;
    }

    public int getServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            serviceCount += serviceMap.get(namespaceId).size();
        }
        return serviceCount;
    }

    public int getInstanceCount() {
        int total = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Service service : serviceMap.get(namespaceId).values()) {
                total += service.allIPs().size();
            }
        }
        return total;
    }

    public int getPagedService(String namespaceId, int startPage, int pageSize, String param, String containedInstance,
                               List<Service> serviceList, boolean hasIpCount) {

        List<Service> matchList;

        if (chooseServiceMap(namespaceId) == null) {
            return 0;
        }

        if (StringUtils.isNotBlank(param)) {
            StringJoiner regex = new StringJoiner(Constants.SERVICE_INFO_SPLITER);
            for (String s : param.split(Constants.SERVICE_INFO_SPLITER, Constants.SERVICE_INFO_SPLIT_COUNT)) {
                regex.add(StringUtils.isBlank(s) ? Constants.ANY_PATTERN
                    : Constants.ANY_PATTERN + s + Constants.ANY_PATTERN);
            }
            matchList = searchServices(namespaceId, regex.toString());
        } else {
            matchList = new ArrayList<>(chooseServiceMap(namespaceId).values());
        }

        if (!CollectionUtils.isEmpty(matchList) && hasIpCount) {
            matchList = matchList.stream().filter(s -> !CollectionUtils.isEmpty(s.allIPs()))
                .collect(Collectors.toList());
        }

        if (StringUtils.isNotBlank(containedInstance)) {

            boolean contained;
            for (int i = 0; i < matchList.size(); i++) {
                Service service = matchList.get(i);
                contained = false;
                List<Instance> instances = service.allIPs();
                for (Instance instance : instances) {
                    if (IPUtil.containsPort(containedInstance)) {
                        if (StringUtils.equals(instance.getIp() + IPUtil.IP_PORT_SPLITER + instance.getPort(),
                            containedInstance)) {
                            contained = true;
                            break;
                        }
                    } else {
                        if (StringUtils.equals(instance.getIp(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    }
                }
                if (!contained) {
                    matchList.remove(i);
                    i--;
                }
            }
        }

        if (pageSize >= matchList.size()) {
            serviceList.addAll(matchList);
            return matchList.size();
        }

        for (int i = 0; i < matchList.size(); i++) {
            if (i < startPage * pageSize) {
                continue;
            }

            serviceList.add(matchList.get(i));

            if (serviceList.size() >= pageSize) {
                break;
            }
        }

        return matchList.size();
    }

    /**
     * Shut down service manager v1.x.
     *
     * @throws NacosException nacos exception during shutdown
     */
    public void shutdown() throws NacosException {
        try {
            for (Map.Entry<String, Map<String, Service>> entry : serviceMap.entrySet()) {
                destroyAllService(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new NacosException(NacosException.SERVER_ERROR, "shutdown serviceManager failed", e);
        }
    }

    private void destroyAllService(String namespace, Map<String, Service> serviceMap) throws Exception {
        for (Map.Entry<String, Service> entry : serviceMap.entrySet()) {
            Service service = entry.getValue();
            String name = service.getName();
            service.destroy();
            String ephemeralInstanceListKey = KeyBuilder.buildInstanceListKey(namespace, name, true);
            String persistInstanceListKey = KeyBuilder.buildInstanceListKey(namespace, name, false);
            consistencyService.unListen(ephemeralInstanceListKey, service);
            consistencyService.unListen(persistInstanceListKey, service);
            consistencyService.unListen(KeyBuilder.buildServiceMetaKey(namespace, name), service);
            Loggers.SRV_LOG.info("[DEAD-SERVICE] {}", service.toJson());
        }
    }

    public static class ServiceChecksum {

        public String namespaceId;

        public Map<String, String> serviceName2Checksum = new HashMap<String, String>();

        public ServiceChecksum() {
            this.namespaceId = Constants.DEFAULT_NAMESPACE_ID;
        }

        public ServiceChecksum(String namespaceId) {
            this.namespaceId = namespaceId;
        }

        /**
         * Add service checksum.
         *
         * @param serviceName service name
         * @param checksum    checksum of service
         */
        public void addItem(String serviceName, String checksum) {
            if (StringUtils.isEmpty(serviceName) || StringUtils.isEmpty(checksum)) {
                Loggers.SRV_LOG.warn("[DOMAIN-CHECKSUM] serviceName or checksum is empty,serviceName: {}, checksum: {}",
                    serviceName, checksum);
                return;
            }
            serviceName2Checksum.put(serviceName, checksum);
        }
    }


    private class ServiceReporter implements Runnable {

        @Override
        public void run() {
            try {

                Map<String, Set<String>> allServiceNames = getAllServiceNames();

                if (allServiceNames.size() <= 0) {
                    //ignore
                    return;
                }

                for (String namespaceId : allServiceNames.keySet()) {

                    ServiceChecksum checksum = new ServiceChecksum(namespaceId);

                    for (String serviceName : allServiceNames.get(namespaceId)) {
                        if (!distroMapper.responsible(serviceName)) {
                            continue;
                        }

                        Service service = getService(namespaceId, serviceName);

                        if (service == null || service.isEmpty()) {
                            continue;
                        }

                        service.recalculateChecksum();

                        checksum.addItem(serviceName, service.getChecksum());
                    }

                    Message msg = new Message();

                    msg.setData(JacksonUtils.toJson(checksum));

                    Collection<Member> sameSiteServers = memberManager.allMembers();

                    if (sameSiteServers == null || sameSiteServers.size() <= 0) {
                        return;
                    }

                    for (Member server : sameSiteServers) {
                        if (server.getAddress().equals(NetUtils.localServer())) {
                            continue;
                        }
                        synchronizer.send(server.getAddress(), msg);
                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[DOMAIN-STATUS] Exception while sending service status", e);
            } finally {
                GlobalExecutor.scheduleServiceReporter(this, switchDomain.getServiceStatusSynchronizationPeriodMillis(),
                    TimeUnit.MILLISECONDS);
            }
        }
    }


    private static class ServiceKey {

        private String namespaceId;

        private String serviceName;

        private String serverIP;

        private String checksum;

        public String getChecksum() {
            return checksum;
        }

        public String getServerIP() {
            return serverIP;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getNamespaceId() {
            return namespaceId;
        }

        public ServiceKey(String namespaceId, String serviceName, String serverIP, String checksum) {
            this.namespaceId = namespaceId;
            this.serviceName = serviceName;
            this.serverIP = serverIP;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return JacksonUtils.toJson(this);
        }
    }
}
