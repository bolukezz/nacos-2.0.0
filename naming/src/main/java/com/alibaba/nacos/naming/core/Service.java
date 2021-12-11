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
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.core.v2.upgrade.doublewrite.delay.DoubleWriteEventListener;
import com.alibaba.nacos.naming.healthcheck.ClientBeatCheckTask;
import com.alibaba.nacos.naming.healthcheck.ClientBeatProcessor;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.alibaba.nacos.naming.selector.NoneSelector;
import com.alibaba.nacos.naming.selector.Selector;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Service of Nacos server side
 *
 * <p>We introduce a 'service --> cluster --> instance' model, in which service stores a list of clusters, which
 * contain a list of instances.
 *
 * <p>his class inherits from Service in API module and stores some fields that do not have to expose to client.
 *
 * @author nkorange
 */
@JsonInclude(Include.NON_NULL)
public class Service extends com.alibaba.nacos.api.naming.pojo.Service implements Record, RecordListener<Instances> {

    private static final String SERVICE_NAME_SYNTAX = "[0-9a-zA-Z@\\.:_-]+";

    @JsonIgnore
    private ClientBeatCheckTask clientBeatCheckTask = new ClientBeatCheckTask(this);

    /**
     * Identify the information used to determine how many isEmpty judgments the service has experienced.
     */
    private int finalizeCount = 0;

    private String token;

    private List<String> owners = new ArrayList<>();

    private Boolean resetWeight = false;

    private Boolean enabled = true;

    private Selector selector = new NoneSelector();

    private String namespaceId;

    /**
     * IP will be deleted if it has not send beat for some time, default timeout is 30 seconds.
     */
    private long ipDeleteTimeout = 30 * 1000;

    private volatile long lastModifiedMillis = 0L;

    private volatile String checksum;

    /**
     * TODO set customized push expire time.
     */
    private long pushCacheMillis = 0L;

    private Map<String, Cluster> clusterMap = new HashMap<>();

    public Service() {
    }

    public Service(String name) {
        super(name);
    }

    @JsonIgnore
    public UdpPushService getPushService() {
        return ApplicationUtils.getBean(UdpPushService.class);
    }

    public long getIpDeleteTimeout() {
        return ipDeleteTimeout;
    }

    public void setIpDeleteTimeout(long ipDeleteTimeout) {
        this.ipDeleteTimeout = ipDeleteTimeout;
    }

    /**
     * Process client beat.
     * 处理客户端的心跳
     * 封装一个ClientBeatProcessor ，然后交给了HealthCheckReactor 的scheduleNamingHealth 方法，其实就是给了一个健康检查的线程池处理了。
     * 看下ClientBeatProcessor 这个任务里面怎样执行的。
     *
     * @param rsInfo metrics info of server
     */
    public void processClientBeat(final RsInfo rsInfo) {
        //创建客户端的beat processor
        ClientBeatProcessor clientBeatProcessor = new ClientBeatProcessor();
        clientBeatProcessor.setService(this);
        clientBeatProcessor.setRsInfo(rsInfo);
        //健康检查，这里交给线程池执行了，我们看下ClientBeatProcessor的run方法
        HealthCheckReactor.scheduleNow(clientBeatProcessor);
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public long getLastModifiedMillis() {
        return lastModifiedMillis;
    }

    public void setLastModifiedMillis(long lastModifiedMillis) {
        this.lastModifiedMillis = lastModifiedMillis;
    }

    public Boolean getResetWeight() {
        return resetWeight;
    }

    public void setResetWeight(Boolean resetWeight) {
        this.resetWeight = resetWeight;
    }

    public Selector getSelector() {
        return selector;
    }

    public void setSelector(Selector selector) {
        this.selector = selector;
    }

    @Override
    public boolean interests(String key) {
        return KeyBuilder.matchInstanceListKey(key, namespaceId, getName());
    }

    @Override
    public boolean matchUnlistenKey(String key) {
        return KeyBuilder.matchInstanceListKey(key, namespaceId, getName());
    }

    //数据改变
    @Override
    public void onChange(String key, Instances value) throws Exception {

        Loggers.SRV_LOG.info("[NACOS-RAFT] datum is changed, key: {}, value: {}", key, value);

        for (Instance instance : value.getInstanceList()) {

            if (instance == null) {
                // Reject this abnormal instance list:
                throw new RuntimeException("got null instance " + key);
            }

            //处理权重问题
            if (instance.getWeight() > 10000.0D) {
                instance.setWeight(10000.0D);
            }

            if (instance.getWeight() < 0.01D && instance.getWeight() > 0.0D) {
                instance.setWeight(0.01D);
            }
        }

        //更新instance,核心的在updateIPs这个方法方法中，参数1是新的instance集合，参数2是是否是临时节点
        updateIPs(value.getInstanceList(), KeyBuilder.matchEphemeralInstanceListKey(key));

        recalculateChecksum();
    }

    @Override
    public void onDelete(String key) throws Exception {
        boolean isEphemeral = KeyBuilder.matchEphemeralInstanceListKey(key);
        for (Cluster each : clusterMap.values()) {
            each.updateIps(Collections.emptyList(), isEphemeral);
        }
    }

    /**
     * Get count of healthy instance in service.
     *
     * @return count of healthy instance
     */
    public int healthyInstanceCount() {

        int healthyCount = 0;
        for (Instance instance : allIPs()) {
            if (instance.isHealthy()) {
                healthyCount++;
            }
        }
        return healthyCount;
    }

    public boolean triggerFlag() {
        return (healthyInstanceCount() * 1.0 / allIPs().size()) <= getProtectThreshold();
    }

    /**
     * Update instances.
     *
     * @param instances instances
     * @param ephemeral whether is ephemeral instance
     */
    //这个方法其实就是遍历instance集合，然后更新clusterMap 这个里面的内容，这个clusterMap 其实就是clusterName 与cluster的对应关系，从代码上可以看到实现弄出所有的cluster，
    // 然后遍历instance集合，如果没有某个instance没有cluster，就设置成默认DEFAULT_CLUSTER_NAME，如果某个cluster没有的话就创建。然后塞到一个cluster与instance集合对应关系的map中。
    //接着就是遍历clusterMap更新下instance列表，这个更新instance列表代码很多，我就不贴出来了，主要思想还是比对新老的，然后找出新的instance，与挂了的instance，注意这一步是更新
    // cluster对象里面的集合，其实就是2个set，一个存临时节点的，一个是存永久节点的。
    //到这我们的服务注册就算是完事了，最后这个service.onChange方法中还有一行代码我们需要注意 getPushService().serviceChanged(this);这个就是service服务列表发生变化，然后进行通知的。
    //
    public void updateIPs(Collection<Instance> instances, boolean ephemeral) {
        Map<String, List<Instance>> ipMap = new HashMap<>(clusterMap.size());
        for (String clusterName : clusterMap.keySet()) {
            ipMap.put(clusterName, new ArrayList<>());
        }

        for (Instance instance : instances) {
            try {
                if (instance == null) {
                    Loggers.SRV_LOG.error("[NACOS-DOM] received malformed ip: null");
                    continue;
                }

                if (StringUtils.isEmpty(instance.getClusterName())) {
                    //cluster是null的话就设置为DEFAULT
                    instance.setClusterName(UtilsAndCommons.DEFAULT_CLUSTER_NAME);
                }

                //cluster不存在的话就创建对应的cluster
                if (!clusterMap.containsKey(instance.getClusterName())) {
                    Loggers.SRV_LOG
                        .warn("cluster: {} not found, ip: {}, will create new cluster with default configuration.",
                            instance.getClusterName(), instance.toJson());
                    Cluster cluster = new Cluster(instance.getClusterName(), this);
                    cluster.init();
                    //塞到map中
                    getClusterMap().put(instance.getClusterName(), cluster);
                }

                List<Instance> clusterIPs = ipMap.get(instance.getClusterName());
                if (clusterIPs == null) {
                    clusterIPs = new LinkedList<>();
                    ipMap.put(instance.getClusterName(), clusterIPs);
                }

                clusterIPs.add(instance);
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[NACOS-DOM] failed to process ip: " + instance, e);
            }
        }

        for (Map.Entry<String, List<Instance>> entry : ipMap.entrySet()) {
            //make every ip mine
            List<Instance> entryIPs = entry.getValue();
            //某个集群，更新集群下面的instance,这个更新instance列表代码很多，我就不贴出来了，主要思想还是比对新老的，然后找出新的instance，与挂了的instance，
            // 注意这一步是更新 cluster对象里面的集合，其实就是2个set，一个存临时节点的，一个是存永久节点的。
            clusterMap.get(entry.getKey()).updateIps(entryIPs, ephemeral);
        }

        //设置最后修改时间
        setLastModifiedMillis(System.currentTimeMillis());
        //获取pushservice，然后服务改变，通知改变，这里的push是UdpPush，然后通过spring的监听者来实现推送
        getPushService().serviceChanged(this);
        ApplicationUtils.getBean(DoubleWriteEventListener.class).doubleWriteToV2(this, ephemeral);
        //这里下面就是在做打印日志
        StringBuilder stringBuilder = new StringBuilder();

        for (Instance instance : allIPs()) {
            stringBuilder.append(instance.toIpAddr()).append("_").append(instance.isHealthy()).append(",");
        }

        Loggers.EVT_LOG.info("[IP-UPDATED] namespace: {}, service: {}, ips: {}", getNamespaceId(), getName(),
            stringBuilder.toString());

    }

    /**
     * Init service.
     * 我们总结一下，首先是心跳机制，客户端服务注册的时候，会生成一个beatInfo，然后添加到beatReactor这个组件中，其实就是添加到任务调度线程池中，每5s向服务端发送一次心跳。
     * 接着就是服务端接收到这个心跳消息之后，就会生成一个ClientBeatProcessor ，然后扔到线程池里面执行，就是找到对应的instance，修改lastbeat 这个字段，如果是不健康状态的话，
     * 就会修改成健康状态，并通知。
     * 最后就是服务故障扫描了，在服务注册的时候，如果没有对应的service对象，就会创建并初始化，在service初始化的过程中，会创建一个task交给这个健康检查组件，
     * 然后这个健康检查组件会扔到任务线程池中，然后是每5s执行一次这个task，这个task里面主要是遍历这个service 下面所有的instance ，然后判断lastbeat 距离当前时间是否超过了15s如果超过15的话，
     * 就会将健康状态的instance标记成不健康，然后告诉pushService进行通知，这个pushService 其实就是专门通知那些订阅这个instance 订阅者或者是观察者，再就是发送站内的instance超时事件
     * 。接着就是判断是否要需要实例过期，默认是需要的，遍历所有的instance，找出lastbeat距离当前时间超过30s的，然后调用deleteIp方法进行服务下线，其实就是组装http服务下线的请求，发送给本机，
     * 好了，到这我们服务续约与服务故障下线就ok了。
     */
    //这里这个初始化有个非常重要的地方就是往健康检查器中添加一个任务，健康检查的任务，这个任务其实就是扫描这个service里面长时间没有心跳的instance（服务实例），然后进行健康状态改变，服务下线
    public void init() {
        //往健康check中添加,这个是重点，服务的故障下线就是通过这个实现的。
        HealthCheckReactor.scheduleCheck(clientBeatCheckTask);
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            //cluster初始化
            entry.getValue().setService(this);
            entry.getValue().init();
        }
    }

    /**
     * Destroy service.
     *
     * @throws Exception exception
     */
    public void destroy() throws Exception {
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            entry.getValue().destroy();
        }
        HealthCheckReactor.cancelCheck(clientBeatCheckTask);
    }

    /**
     * Judge whether service has instance.
     *
     * @return true if no instance, otherwise false
     */
    public boolean isEmpty() {
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            final Cluster cluster = entry.getValue();
            if (!cluster.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get all instance.
     *
     * @return list of all instance
     */
    public List<Instance> allIPs() {
        List<Instance> result = new ArrayList<>();
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            result.addAll(entry.getValue().allIPs());
        }

        return result;
    }

    /**
     * Get all instance of ephemeral or consistency.
     *
     * @param ephemeral whether ephemeral instance
     * @return all instance of ephemeral if @param ephemeral = true, otherwise all instance of consistency
     */
    public List<Instance> allIPs(boolean ephemeral) {
        List<Instance> result = new ArrayList<>();
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            result.addAll(entry.getValue().allIPs(ephemeral));
        }

        return result;
    }

    /**
     * Get all instance from input clusters.
     *
     * @param clusters cluster names
     * @return all instance from input clusters.
     */
    public List<Instance> allIPs(List<String> clusters) {
        List<Instance> result = new ArrayList<>();
        for (String cluster : clusters) {
            Cluster clusterObj = clusterMap.get(cluster);
            if (clusterObj == null) {
                continue;
            }

            result.addAll(clusterObj.allIPs());
        }
        return result;
    }

    /**
     * Get all instance from input clusters.
     *
     * @param clusters cluster names
     * @return all instance from input clusters, if clusters is empty, return all cluster
     */
    public List<Instance> srvIPs(List<String> clusters) {
        if (CollectionUtils.isEmpty(clusters)) {
            clusters = new ArrayList<>();
            clusters.addAll(clusterMap.keySet());
        }
        return allIPs(clusters);
    }

    public String toJson() {
        return JacksonUtils.toJson(this);
    }

    @JsonIgnore
    public String getServiceString() {
        Map<Object, Object> serviceObject = new HashMap<Object, Object>(10);
        Service service = this;

        serviceObject.put("name", service.getName());

        List<Instance> ips = service.allIPs();
        int invalidIpCount = 0;
        int ipCount = 0;
        for (Instance ip : ips) {
            if (!ip.isHealthy()) {
                invalidIpCount++;
            }

            ipCount++;
        }

        serviceObject.put("ipCount", ipCount);
        serviceObject.put("invalidIPCount", invalidIpCount);

        serviceObject.put("owners", service.getOwners());
        serviceObject.put("token", service.getToken());

        serviceObject.put("protectThreshold", service.getProtectThreshold());

        List<Object> clustersList = new ArrayList<Object>();

        for (Map.Entry<String, Cluster> entry : service.getClusterMap().entrySet()) {
            Cluster cluster = entry.getValue();

            Map<Object, Object> clusters = new HashMap<Object, Object>(10);
            clusters.put("name", cluster.getName());
            clusters.put("healthChecker", cluster.getHealthChecker());
            clusters.put("defCkport", cluster.getDefCkport());
            clusters.put("defIPPort", cluster.getDefIPPort());
            clusters.put("useIPPort4Check", cluster.isUseIPPort4Check());
            clusters.put("sitegroup", cluster.getSitegroup());

            clustersList.add(clusters);
        }

        serviceObject.put("clusters", clustersList);

        try {
            return JacksonUtils.toJson(serviceObject);
        } catch (Exception e) {
            throw new RuntimeException("Service toJson failed", e);
        }
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }

    public Map<String, Cluster> getClusterMap() {
        return clusterMap;
    }

    public void setClusterMap(Map<String, Cluster> clusterMap) {
        this.clusterMap = clusterMap;
    }

    public String getNamespaceId() {
        return namespaceId;
    }

    public void setNamespaceId(String namespaceId) {
        this.namespaceId = namespaceId;
    }

    /**
     * Update from other service.
     *
     * @param vDom other service
     */
    public void update(Service vDom) {

        if (!StringUtils.equals(token, vDom.getToken())) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, token: {} -> {}", getName(), token, vDom.getToken());
            token = vDom.getToken();
        }

        if (!ListUtils.isEqualList(owners, vDom.getOwners())) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, owners: {} -> {}", getName(), owners, vDom.getOwners());
            owners = vDom.getOwners();
        }

        if (getProtectThreshold() != vDom.getProtectThreshold()) {
            Loggers.SRV_LOG
                .info("[SERVICE-UPDATE] service: {}, protectThreshold: {} -> {}", getName(), getProtectThreshold(),
                    vDom.getProtectThreshold());
            setProtectThreshold(vDom.getProtectThreshold());
        }

        if (resetWeight != vDom.getResetWeight().booleanValue()) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, resetWeight: {} -> {}", getName(), resetWeight,
                vDom.getResetWeight());
            resetWeight = vDom.getResetWeight();
        }

        if (enabled != vDom.getEnabled().booleanValue()) {
            Loggers.SRV_LOG
                .info("[SERVICE-UPDATE] service: {}, enabled: {} -> {}", getName(), enabled, vDom.getEnabled());
            enabled = vDom.getEnabled();
        }

        selector = vDom.getSelector();

        setMetadata(vDom.getMetadata());

        updateOrAddCluster(vDom.getClusterMap().values());
        remvDeadClusters(this, vDom);

        Loggers.SRV_LOG.info("cluster size, new: {}, old: {}", getClusterMap().size(), vDom.getClusterMap().size());

        recalculateChecksum();
        ApplicationUtils.getBean(DoubleWriteEventListener.class)
            .doubleWriteMetadataToV2(this, vDom.allIPs(false).isEmpty());
    }

    @Override
    public String getChecksum() {
        if (StringUtils.isEmpty(checksum)) {
            recalculateChecksum();
        }

        return checksum;
    }

    /**
     * Re-calculate checksum of service.
     */
    public synchronized void recalculateChecksum() {
        List<Instance> ips = allIPs();

        StringBuilder ipsString = new StringBuilder();
        ipsString.append(getServiceString());

        if (Loggers.SRV_LOG.isDebugEnabled()) {
            Loggers.SRV_LOG.debug("service to json: " + getServiceString());
        }

        if (CollectionUtils.isNotEmpty(ips)) {
            Collections.sort(ips);
        }

        for (Instance ip : ips) {
            String string = ip.getIp() + ":" + ip.getPort() + "_" + ip.getWeight() + "_" + ip.isHealthy() + "_" + ip
                .getClusterName();
            ipsString.append(string);
            ipsString.append(",");
        }

        checksum = MD5Utils.md5Hex(ipsString.toString(), Constants.ENCODE);
    }

    private void updateOrAddCluster(Collection<Cluster> clusters) {
        for (Cluster cluster : clusters) {
            Cluster oldCluster = clusterMap.get(cluster.getName());
            if (oldCluster != null) {
                oldCluster.setService(this);
                oldCluster.update(cluster);
            } else {
                cluster.init();
                cluster.setService(this);
                clusterMap.put(cluster.getName(), cluster);
            }
        }
    }

    private void remvDeadClusters(Service oldDom, Service newDom) {
        Collection<Cluster> oldClusters = oldDom.getClusterMap().values();
        Collection<Cluster> newClusters = newDom.getClusterMap().values();
        List<Cluster> deadClusters = (List<Cluster>) CollectionUtils.subtract(oldClusters, newClusters);
        for (Cluster cluster : deadClusters) {
            oldDom.getClusterMap().remove(cluster.getName());

            cluster.destroy();
        }
    }

    public int getFinalizeCount() {
        return finalizeCount;
    }

    public void setFinalizeCount(int finalizeCount) {
        this.finalizeCount = finalizeCount;
    }

    public void addCluster(Cluster cluster) {
        clusterMap.put(cluster.getName(), cluster);
    }

    /**
     * Judge whether service is validate.
     *
     * @throws IllegalArgumentException if service is not validate
     */
    public void validate() {
        if (!getName().matches(SERVICE_NAME_SYNTAX)) {
            throw new IllegalArgumentException(
                "dom name can only have these characters: 0-9a-zA-Z-._:, current: " + getName());
        }
        for (Cluster cluster : clusterMap.values()) {
            cluster.validate();
        }
    }
}
