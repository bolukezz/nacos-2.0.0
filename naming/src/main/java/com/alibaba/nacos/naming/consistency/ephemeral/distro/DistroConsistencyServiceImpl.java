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

package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.Objects;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpCombinedKey;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.v2.upgrade.UpgradeJudgement;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 *
 * <p>Use a distro algorithm to divide data into many blocks. Each Nacos server node takes responsibility for exactly
 * one block of data. Each block of data is generated, removed and synchronized by its responsible server. So every
 * Nacos server only handles writings for a subset of the total service data.
 *
 * <p>At mean time every Nacos server receives data sync of other Nacos server, so every Nacos server will eventually
 * have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 */
@DependsOn("ProtocolManager")
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService, DistroDataProcessor {

    private final DistroMapper distroMapper;

    private final DataStore dataStore;

    private final Serializer serializer;

    private final SwitchDomain switchDomain;

    private final GlobalConfig globalConfig;

    private final DistroProtocol distroProtocol;

    private volatile Notifier notifier = new Notifier();

    private Map<String, ConcurrentLinkedQueue<RecordListener>> listeners = new ConcurrentHashMap<>();

    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    public DistroConsistencyServiceImpl(DistroMapper distroMapper, DataStore dataStore, Serializer serializer,
                                        SwitchDomain switchDomain, GlobalConfig globalConfig, DistroProtocol distroProtocol) {
        this.distroMapper = distroMapper;
        this.dataStore = dataStore;
        this.serializer = serializer;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.distroProtocol = distroProtocol;
    }

    @PostConstruct
    public void init() {
        //在初始化的时候提交了一个任务,看下notifier的run放啊
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }

    @Override
    //这里的Record是instance的父类
    public void put(String key, Record value) throws NacosException {
        //往本地里面存储
        onPut(key, value);
        // If upgrade to 2.0.X, do not sync for v1.
        if (ApplicationUtils.getBean(UpgradeJudgement.class).isUseGrpcFeatures()) {
            return;
        }
        //这个玩意很可能就是同步
        distroProtocol.sync(new DistroKey(key, KeyBuilder.INSTANCE_LIST_KEY_PREFIX), DataOperation.CHANGE,
            globalConfig.getTaskDispatchPeriod() / 2);
    }

    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        listeners.remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    /**
     * Put a new record.
     *
     * @param key   key of record
     * @param value record
     */
    //这里需要判断一下是否是临时节点，如果是的话，就封装一个Datum，这个东西就是个kv，不用太多care它，接着就是调用datastore 的put方法进行存储。
    //最后如果有这个key的监听器的话，就会接着往下走，没有的话就返回，我们在初始化service 的时候是注册了2个监听器的，往上翻翻就可以看到。接着就是调用notifier添加任务。
    //先看下这个datastore
    public void onPut(String key, Record value) {

        //判断是否是临时
        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
            //封装datum
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            //自增
            datum.timestamp.incrementAndGet();
            //放到datastore中进行存储
            dataStore.put(key, datum);
        }

        //如果listener里面没有这个key的话直接返回，这个玩意是发布订阅的东西，进行通知，这个玩意service在初始化的会添加进去
        if (!listeners.containsKey(key)) {
            return;
        }
        //添加通知任务
        notifier.addTask(key, DataOperation.CHANGE);
    }

    /**
     * Remove a record.
     *
     * @param key key of record
     */
    public void onRemove(String key) {

        dataStore.remove(key);

        if (!listeners.containsKey(key)) {
            return;
        }

        notifier.addTask(key, DataOperation.DELETE);
    }

    /**
     * Check sum when receive checksums request.
     *
     * @param checksumMap map of checksum
     * @param server      source server request checksum
     */
    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

        if (syncChecksumTasks.containsKey(server)) {
            // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }

        syncChecksumTasks.put(server, "1");

        try {

            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // abort the procedure:
                    return;
                }

                if (!dataStore.contains(entry.getKey()) || dataStore.get(entry.getKey()).value == null || !dataStore
                    .get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }

            for (String key : dataStore.keys()) {

                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }

                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }

            Loggers.DISTRO
                .info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);

            for (String key : toRemoveKeys) {
                onRemove(key);
            }

            if (toUpdateKeys.isEmpty()) {
                return;
            }

            try {
                DistroHttpCombinedKey distroKey = new DistroHttpCombinedKey(KeyBuilder.INSTANCE_LIST_KEY_PREFIX,
                    server);
                distroKey.getActualResourceTypes().addAll(toUpdateKeys);
                DistroData remoteData = distroProtocol.queryFromRemote(distroKey);
                if (null != remoteData) {
                    processData(remoteData.getContent());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }
    }

    private boolean processData(byte[] data) throws Exception {
        if (data.length > 0) {
            Map<String, Datum<Instances>> datumMap = serializer.deserializeMap(data, Instances.class);

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                dataStore.put(entry.getKey(), entry.getValue());

                if (!listeners.containsKey(entry.getKey())) {
                    // pretty sure the service not exist:
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();

                        // The Listener corresponding to the key value must not be empty
                        RecordListener listener = listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).peek();
                        if (Objects.isNull(listener)) {
                            return false;
                        }
                        listener.onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

                if (!listeners.containsKey(entry.getKey())) {
                    // Should not happen:
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }

                try {
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }

                // Update data store if listener executed successfully:
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
        return true;
    }

    @Override
    public boolean processData(DistroData distroData) {
        DistroHttpData distroHttpData = (DistroHttpData) distroData;
        Datum<Instances> datum = (Datum<Instances>) distroHttpData.getDeserializedContent();
        onPut(datum.key, datum.value);
        return true;
    }

    @Override
    public String processType() {
        return KeyBuilder.INSTANCE_LIST_KEY_PREFIX;
    }

    @Override
    public boolean processVerifyData(DistroData distroData, String sourceAddress) {
        DistroHttpData distroHttpData = (DistroHttpData) distroData;
        Map<String, String> verifyData = (Map<String, String>) distroHttpData.getDeserializedContent();
        onReceiveChecksums(verifyData, sourceAddress);
        return true;
    }

    @Override
    public boolean processSnapshot(DistroData distroData) {
        try {
            return processData(distroData.getContent());
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            listeners.put(key, new ConcurrentLinkedQueue<>());
        }

        if (listeners.get(key).contains(listener)) {
            return;
        }

        listeners.get(key).add(listener);
    }

    @Override
    public void unListen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            return;
        }
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return distroProtocol.isInitialized() || !globalConfig.isDataWarmup();
    }

    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair<String, DataOperation>> tasks = new ArrayBlockingQueue<>(1024 * 1024);

        /**
         * Add new notify task to queue.
         * 添加通知任务到队列中
         *
         * @param datumKey data key
         * @param action   action for data
         */
        /**
         * 这里其实就是往任务队列中添加了一个任务。到这按理说我们服务注册就该结束了，但是，我们发现生成了新的instance集合并没有更新到service对象里面去，
         * 所以还得继续往下看，看看这个通知任务是怎么回事。
         * 其实DistroConsistencyServiceImpl 这个类在初始化的时候，然后提交了一个任务
         */
        public void addTask(String datumKey, DataOperation action) {
            //如果已经存在，并且是change事件
            if (services.containsKey(datumKey) && action == DataOperation.CHANGE) {
                return;
            }
            //如果是change事件
            if (action == DataOperation.CHANGE) {
                //往这个缓存中放一份
                services.put(datumKey, StringUtils.EMPTY);
            }
            //往任务队列中放
            tasks.offer(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");

            //就是取出任务队列中的任务，调用handle方法进行处理
            for (; ; ) {
                try {
                    //取出任务
                    Pair<String, DataOperation> pair = tasks.take();
                    //处理任务
                    handle(pair);
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }

        //这里直接通知，调用listener 的onChange活着onDelete执行相关的工作。我们那个时候是将service作为listener注册进来了，所以我们看下service的onChange方法
        private void handle(Pair<String, DataOperation> pair) {
            try {
                String datumKey = pair.getValue0();
                DataOperation action = pair.getValue1();

                //就是从这个缓存中移除
                services.remove(datumKey);

                int count = 0;

                if (!listeners.containsKey(datumKey)) {
                    return;
                }

                for (RecordListener listener : listeners.get(datumKey)) {

                    count++;

                    try {
                        //通知数据已经变更
                        if (action == DataOperation.CHANGE) {//已经改变
                            listener.onChange(datumKey, dataStore.get(datumKey).value);
                            continue;
                        }

                        //通知数据删除
                        if (action == DataOperation.DELETE) {//删除
                            listener.onDelete(datumKey);
                            continue;
                        }
                    } catch (Throwable e) {
                        Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                    }
                }

                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO
                        .debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                            datumKey, count, action.name());
                }
            } catch (Throwable e) {
                Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
            }
        }
    }
}
