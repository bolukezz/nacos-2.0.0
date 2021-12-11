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

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.naming.healthcheck.heartbeat.BeatProcessor;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thread to update ephemeral instance triggered by client beat for v1.x.
 *
 * @author nkorange
 */
public class ClientBeatProcessor implements BeatProcessor {

    public static final long CLIENT_BEAT_TIMEOUT = TimeUnit.SECONDS.toMillis(15);

    private RsInfo rsInfo;

    private Service service;

    @JsonIgnore
    public UdpPushService getPushService() {
        return ApplicationUtils.getBean(UdpPushService.class);
    }

    public RsInfo getRsInfo() {
        return rsInfo;
    }

    public void setRsInfo(RsInfo rsInfo) {
        this.rsInfo = rsInfo;
    }

    public Service getService() {
        return service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    @Override
    public void run() {
        Service service = this.service;
        //就是判断 事件log是否开启，使用参数com.alibaba.nacos.naming.event开启
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[CLIENT-BEAT] processing beat: {}", rsInfo.toString());
        }

        String ip = rsInfo.getIp();
        String clusterName = rsInfo.getCluster();
        int port = rsInfo.getPort();
        //获取到对应的cluster
        Cluster cluster = service.getClusterMap().get(clusterName);
        //获取到这个集群下面所有的instance
        List<Instance> instances = cluster.allIPs(true);

        for (Instance instance : instances) {
            //就是找 ip和 port 能够对应上的instance
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[CLIENT-BEAT] refresh beat: {}", rsInfo.toString());
                }
                //设置心跳间隔，这个也就算是续约了
                //其实就是通过namespace/serviceName/cluster/ip/port找到对应的instance对象，重新设置一下LastBeat 的时间，也就是
                //instance.setLastBeat(System.currentTimeMillis());
                instance.setLastBeat(System.currentTimeMillis());
                if (!instance.isMarked()) {
                    //如果不健康的话
                    if (!instance.isHealthy()) {
                        //设置健康
                        instance.setHealthy(true);
                        Loggers.EVT_LOG
                            .info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: client beat ok",
                                cluster.getService().getName(), ip, port, cluster.getName(),
                                UtilsAndCommons.LOCALHOST_SITE);
                        //service状态改变,这行需要注意下，健康状态改变了，会引起它 将新的instance信息推送到那堆服务订阅者客户端上
                        //可以看到，处理心跳消息也是异步的，将处理封装成task投寄到线程池，然后就直接返回给客户端了，由线程池执行这个task。
                        getPushService().serviceChanged(service);
                    }
                }
            }
        }
    }
}
