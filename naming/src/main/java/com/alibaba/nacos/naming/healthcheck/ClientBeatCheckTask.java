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

import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.v2.upgrade.UpgradeJudgement;
import com.alibaba.nacos.naming.healthcheck.heartbeat.BeatCheckTask;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingProxy;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Client beat check task of service for version 1.x.
 *
 * @author nkorange
 */
public class ClientBeatCheckTask implements BeatCheckTask {

    private Service service;

    public ClientBeatCheckTask(Service service) {
        this.service = service;
    }

    @JsonIgnore
    public UdpPushService getPushService() {
        return ApplicationUtils.getBean(UdpPushService.class);
    }

    @JsonIgnore
    public DistroMapper getDistroMapper() {
        return ApplicationUtils.getBean(DistroMapper.class);
    }

    public GlobalConfig getGlobalConfig() {
        return ApplicationUtils.getBean(GlobalConfig.class);
    }

    public SwitchDomain getSwitchDomain() {
        return ApplicationUtils.getBean(SwitchDomain.class);
    }

    @Override
    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName());
    }

    /*
    这部分主要是获取到这个service下面所有的instance，然后遍历这堆instance，判断如果instance的lastbeat时间距离当前时间已经超过了15s，
    也就是preserved.heart.beat.timeout 这个参数控制的，如果没有被标记，就会检查健康状态，如果是健康就更改健康状态，并通知，并且推送实例心跳超时时间
     */
    @Override
    public void run() {
        try {
            // If upgrade to 2.0.X stop health check with v1
            if (ApplicationUtils.getBean(UpgradeJudgement.class).isUseGrpcFeatures()) {
                return;
            }
            if (!getDistroMapper().responsible(service.getName())) {
                return;
            }

            if (!getSwitchDomain().isHealthCheckEnabled()) {
                return;
            }
            //获取service下面所有的instance
            List<Instance> instances = service.allIPs(true);

            // first set health status of instances:
            //遍历
            for (Instance instance : instances) {
                //这个参数配置超时时间， preserved.heart.beat.timeout，然后默认是15s
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
                    //超时
                    if (!instance.isMarked()) {//是否被标记，如果没有的话
                        if (instance.isHealthy()) { //如果是健康状态就设置为false
                            instance.setHealthy(false);
                            Loggers.EVT_LOG
                                .info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                                    instance.getIp(), instance.getPort(), instance.getClusterName(),
                                    service.getName(), UtilsAndCommons.LOCALHOST_SITE,
                                    instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
                            //状态改变
                            getPushService().serviceChanged(service);
                        }
                    }
                }
            }

            /**
             * 这部分就是判断是否过期instance，默认是true的，就是会过期实例（这里比较拗口，其实就是判断参数要不要过期服务，如果是false的话，就算是长时间没有心跳，也不会摘掉服务）
             * 接着就是遍历所有的实例instance，如果已经被标记就跳过，如果没有的话，判断实例的lastBeat时间距离现在时间是否超过了30s，如果查过的话，就走deleteIp删除这个实例
             */
            if (!getGlobalConfig().isExpireInstance()) {
                return;
            }

            // then remove obsolete instances:
            //删除过时的服务，默认是30s，没有心跳
            for (Instance instance : instances) {

                if (instance.isMarked()) {//如果已经被标记，就跳过
                    continue;
                }

                //删除超时时间，，getIpDeleteTimeout默认是30s
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
                    // delete instance
                    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(),
                        JacksonUtils.toJson(instance));
                    //删除实例,看下这个方法
                    deleteIp(instance);
                }
            }

        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }

    }

    //可以看到这个deleteIp 方法其实就是组装了个http请求，然后向本机发送服务下线请求，可以看到组装请求参数，然后请求url是“http://127.0.0.1:端口/context/v1/ns/instance”
    // 走的是delete 请求方式，往下我们就不看了
    private void deleteIp(Instance instance) {

        try {
            //这里其实是发送http请求到本地，然后进行服务下线
            NamingProxy.Request request = NamingProxy.Request.newRequest();
            request.appendParam("ip", instance.getIp()).appendParam("port", String.valueOf(instance.getPort()))
                .appendParam("ephemeral", "true").appendParam("clusterName", instance.getClusterName())
                .appendParam("serviceName", service.getName()).appendParam("namespaceId", service.getNamespaceId());

            String url = "http://" + IPUtil.localHostIP() + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort() + EnvUtil
                .getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();

            // delete instance asynchronously:
            //异步发送请求下线
            HttpClient.asyncHttpDelete(url, null, null, new Callback<String>() {
                @Override
                public void onReceive(RestResult<String> result) {
                    if (!result.ok()) {
                        Loggers.SRV_LOG
                            .error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                                instance.toJson(), result.getMessage(), result.getCode());
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    Loggers.SRV_LOG
                        .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(),
                            throwable);
                }

                @Override
                public void onCancel() {

                }
            });

        } catch (Exception e) {
            Loggers.SRV_LOG
                .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(), e);
        }
    }
}