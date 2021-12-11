/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.push.v1;

import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.push.NamingSubscriberService;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Naming subscriber service for v1.x.
 *
 * @author xiweng.yy
 * @deprecated Will be removed in v2.1.x version
 */
@org.springframework.stereotype.Service
@Deprecated
public class NamingSubscriberServiceV1Impl implements NamingSubscriberService {

    private final ConcurrentMap<String, ConcurrentMap<String, PushClient>> clientMap = new ConcurrentHashMap<>();

    public NamingSubscriberServiceV1Impl() {
        GlobalExecutor.scheduleRetransmitter(() -> {
            try {
                removeClientIfZombie();
            } catch (Throwable e) {
                Loggers.PUSH.warn("[NACOS-PUSH] failed to remove client zombie");
            }
        }, 0, 20, TimeUnit.SECONDS);
    }

    private void removeClientIfZombie() {
        int size = 0;
        for (Map.Entry<String, ConcurrentMap<String, PushClient>> entry : clientMap.entrySet()) {
            ConcurrentMap<String, PushClient> clientConcurrentMap = entry.getValue();
            for (Map.Entry<String, PushClient> entry1 : clientConcurrentMap.entrySet()) {
                PushClient client = entry1.getValue();
                if (client.zombie()) {
                    clientConcurrentMap.remove(entry1.getKey());
                }
            }
            size += clientConcurrentMap.size();
        }
        if (Loggers.PUSH.isDebugEnabled()) {
            Loggers.PUSH.debug("[NACOS-PUSH] clientMap size: {}", size);
        }
    }

    public ConcurrentMap<String, ConcurrentMap<String, PushClient>> getClientMap() {
        return clientMap;
    }

    @Override
    public Collection<Subscriber> getSubscribers(String namespaceId, String serviceName) {
        String serviceKey = UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName);
        ConcurrentMap<String, PushClient> clientConcurrentMap = clientMap.get(serviceKey);
        if (Objects.isNull(clientConcurrentMap)) {
            return Collections.emptyList();
        }
        Collection<Subscriber> result = new ArrayList<>();
        clientConcurrentMap.forEach((key, client) -> {
            result.add(
                    new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(), client.getIp(), namespaceId,
                            serviceName, client.getPort()));
        });
        return result;
    }

    @Override
    public Collection<Subscriber> getSubscribers(Service service) {
        return getSubscribers(service.getNamespace(), service.getGroupedServiceName());
    }

    @Override
    public Collection<Subscriber> getFuzzySubscribers(String namespaceId, String serviceName) {
        Collection<Subscriber> result = new ArrayList<>();
        clientMap.forEach((outKey, clientConcurrentMap) -> {
            //get groupedName from key
            String serviceFullName = outKey.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[1];
            //get groupName
            String groupName = NamingUtils.getGroupName(serviceFullName);
            //get serviceName
            String name = NamingUtils.getServiceName(serviceFullName);
            //fuzzy match
            if (outKey.startsWith(namespaceId) && name.indexOf(NamingUtils.getServiceName(serviceName)) >= 0
                    && groupName.indexOf(NamingUtils.getGroupName(serviceName)) >= 0) {
                clientConcurrentMap.forEach((key, client) -> {
                    result.add(new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(), client.getIp(),
                            namespaceId, serviceFullName, client.getPort()));
                });
            }
        });
        return result;
    }

    @Override
    public Collection<Subscriber> getFuzzySubscribers(Service service) {
        return getFuzzySubscribers(service.getNamespace(), service.getGroupedServiceName());
    }

    /**
     * Add push target client.
     *
     * @param namespaceId namespace id
     * @param serviceName service name
     * @param clusters    cluster
     * @param agent       agent information
     * @param socketAddr  client address
     * @param dataSource  datasource of push data
     * @param tenant      tenant
     * @param app         app
     */
    public void addClient(String namespaceId, String serviceName, String clusters, String agent,
            InetSocketAddress socketAddr, DataSource dataSource, String tenant, String app) {
        //创建client
        PushClient client = new PushClient(namespaceId, serviceName, clusters, agent, socketAddr, dataSource, tenant,
                app);
        //然后添加
        addClient(client);
    }

    /**
     * Add push target client.
     *这个方法就是先生成一个serviceKey ，然后去clientMap中获取，如果没有这个clients话就创建塞到这个clientMap 中，
     * 最后就是将PushClient 转成字符串当作key，去clients这个map中获取，如果没有，就添加进去，如果有的话就只是刷新一下旧的。好了，
     * 到这就把订阅信息添加到PushService这个组件里面了，这个时候服务端就可以返回给客户端了。
     * @param client push target client
     */
    public void addClient(PushClient client) {
        // client is stored by key 'serviceName' because notify event is driven by serviceName change
        String serviceKey = UtilsAndCommons.assembleFullServiceName(client.getNamespaceId(), client.getServiceName());
        ConcurrentMap<String, PushClient> clients = clientMap.get(serviceKey);
        //之前没有过
        if (clients == null) {
            clientMap.putIfAbsent(serviceKey, new ConcurrentHashMap<>(1024));
            clients = clientMap.get(serviceKey);
        }

        //如果有老的话，就刷新一下老的
        PushClient oldClient = clients.get(client.toString());
        if (oldClient != null) {
            //其实就是更新下lastRefTime这个字段
            oldClient.refresh();
        } else {
            //如果之前没有添加过，添加进去就可以了。
            PushClient res = clients.putIfAbsent(client.toString(), client);
            if (res != null) {
                Loggers.PUSH.warn("client: {} already associated with key {}", res.getAddrStr(), res.toString());
            }
            Loggers.PUSH.debug("client: {} added for serviceName: {}", client.getAddrStr(), client.getServiceName());
        }
    }
}
