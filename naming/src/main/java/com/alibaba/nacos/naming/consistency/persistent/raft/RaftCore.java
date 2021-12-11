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

package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.EventPublisher;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ValueChangeEvent;
import com.alibaba.nacos.naming.consistency.persistent.ClusterVersionJudgement;
import com.alibaba.nacos.naming.consistency.persistent.PersistentNotifier;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

/**
 * Raft core code.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@DependsOn("ProtocolManager")
@Component
public class RaftCore implements Closeable {

    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

    public static final Lock OPERATE_LOCK = new ReentrantLock();

    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;

    private volatile ConcurrentMap<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

    private RaftPeerSet peers;

    private final SwitchDomain switchDomain;

    private final GlobalConfig globalConfig;

    private final RaftProxy raftProxy;

    private final RaftStore raftStore;

    private final ClusterVersionJudgement versionJudgement;

    public final PersistentNotifier notifier;

    private final EventPublisher publisher;

    private final RaftListener raftListener;

    private boolean initialized = false;

    private volatile boolean stopWork = false;

    private ScheduledFuture masterTask = null;

    private ScheduledFuture heartbeatTask = null;

    public RaftCore(RaftPeerSet peers, SwitchDomain switchDomain, GlobalConfig globalConfig, RaftProxy raftProxy,
                    RaftStore raftStore, ClusterVersionJudgement versionJudgement, RaftListener raftListener) {
        this.peers = peers;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.raftProxy = raftProxy;
        this.raftStore = raftStore;
        this.versionJudgement = versionJudgement;
        this.notifier = new PersistentNotifier(key -> null == getDatum(key) ? null : getDatum(key).value);
        this.publisher = NotifyCenter.registerToPublisher(ValueChangeEvent.class, 16384);
        this.raftListener = raftListener;
    }

    /**
     * Init raft core.
     * 首先是将文件中的数据加载到内存中，文件就是那些服务信息，实例列表这些东西。在你nacos.home/data/naming/data/{namespace}/ 下面。
     * 接着就是加载 term ，将 nacos_home/data/naming/data/meta.properties文件中的term加载到内存中，需要注意的每个peer都有自己的term，
     * 这个term很重要，在nacos中能不能选上leader就是靠的term大小。再就是创建了2个任务放到调度线程池中了，一个是选举任务，一个是心跳任务。
     *
     * @throws Exception any exception during init
     */
    @PostConstruct
    public void init() throws Exception {
        Loggers.RAFT.info("initializing Raft sub-system");
        final long start = System.currentTimeMillis();

        //加载数据
        raftStore.loadDatums(notifier, datums);

        //加载 nacos_home/data/naming/data/meta.properties这个term
        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));

        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());

        initialized = true;

        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));

        //注册一个选举任务，500ms执行一次，看下选举任务的run方法
        masterTask = GlobalExecutor.registerMasterElection(new MasterElection());
        //注册一个心跳任务，也是500ms执行一次
        heartbeatTask = GlobalExecutor.registerHeartbeat(new HeartBeat());

        //同版本切换注册一个观察者，如果切换了新版本，就会停止这个core
        versionJudgement.registerObserver(isAllNewVersion -> {
            stopWork = isAllNewVersion;
            if (stopWork) {
                try {
                    shutdown();
                    raftListener.removeOldRaftMetadata();
                } catch (NacosException e) {
                    throw new NacosRuntimeException(NacosException.SERVER_ERROR, e);
                }
            }
        }, 100);

        //这册一个监听器订阅者
        NotifyCenter.registerSubscriber(notifier);

        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
            GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }

    public Map<String, ConcurrentHashSet<RecordListener>> getListeners() {
        return notifier.getListeners();
    }

    /**
     * Signal publish new record. If not leader, signal to leader. If leader, try to commit publish.
     *
     * @param key   key
     * @param value value
     * @throws Exception any exception during publish
     */
    //比较重要的点，判断，如果本机不是leader节点的话，就要封装参数将请求发送给leader节点，leader 节点收到请求后，其实还是走到这个put方法中。来看下这个raftProxy.proxyPostLarge 方法
    public void signalPublish(String key, Record value) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        //不是leader的话
        if (!isLeader()) {
            ObjectNode params = JacksonUtils.createEmptyJsonNode();
            params.put("key", key);
            params.replace("value", JacksonUtils.transferToJsonNode(value));
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);

            final RaftPeer leader = getLeader();
            //发送给leader  /raft/datum post请求
            raftProxy.proxyPostLarge(leader.ip, API_PUB, params.toString(), parameters);
            return;
        }

        //如果是leader的话
        OPERATE_LOCK.lock();
        try {
            final long start = System.currentTimeMillis();
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            //原先没有
            if (getDatum(key) == null) {
                datum.timestamp.set(1L);
            } else {
                //有了的话timestamp+1
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }

            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            //数据
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            //来源
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));

            //本地存储
            //这里有2个重要的点，一个是更新数据里面的timestamp 值，就是自增1，这个timestamp 值很重要，相当于是个版本
            // 。接着调用onPublish 方法进行本地存储。我们来看下onPublish方法干了啥
            onPublish(datum, peers.local());
            //将内容同步给所有的FOLLOWER节点
            final String content = json.toString();


            //这块也是很重要的，这里用了CountDownLatch 来计数堵塞超时，数量就是节点数/2+1（节点半数+1），接着就是遍历这堆节点了，然后发送数据同步请求，
            // 然后每收到一个follower节点的回复，CountDownLatch就减1 一下。
            //可以看到后面一直等着，5秒超时，如果5s之内follower节点回复数量大于等于 半数+1，就算成功了，如果没有的话，就抛出异常，然后释放锁资源。
            //这里可以仔细想一下，leader节点是先将数据保存到本地了，不管是内存还是文件，term也进行了+1 ，更新事件也发布了，这个时候，
            // 如果因为网络故障同步节点成功达不到这个半数+1 ，就会造成数据不一致的情况，客户端将请求打到不同的nacos 节点上，拿到的数据是不一致的
            // ，所以从这里可以看出来nacos这个raft 协议并不是强一致，强一致的就要么都成功，要么都失败的，它这个整的失败的时候数据，leader与follower数据发生了不一致。
            // 其实这个nacos 的raft 协议实现的是个最终一致。接下来我们看下它最终一致是怎样实现的。
            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            for (final String server : peers.allServersIncludeMyself()) {
                if (isLeader(server)) {
                    latch.countDown();
                    continue;
                }
                final String url = buildUrl(server, API_ON_PUB);
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key", key), content, new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                .warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}",
                                    datum.key, server, result.getCode());
                            return;
                        }
                        //说明这个peer 回复ok了
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to publish data to peer", throwable);
                    }

                    @Override
                    public void onCancel() {

                    }
                });

            }

            //5s后还没有完事就抛出异常
            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // only majority servers return success can we consider this update success
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }

            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * Signal delete record. If not leader, signal leader delete. If leader, try to commit delete.
     *
     * @param key key
     * @throws Exception any exception during delete
     */
    public void signalDelete(final String key) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        OPERATE_LOCK.lock();
        try {

            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }

            // construct datum:
            Datum datum = new Datum();
            datum.key = key;
            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));

            onDelete(datum.key, peers.local());

            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildUrl(server, API_ON_DEL);
                HttpClient.asyncHttpDeleteLarge(url, null, json.toString(), new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                .warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}",
                                    key, server, result.getCode());
                            return;
                        }

                        RaftPeer local = peers.local();

                        local.resetLeaderDue();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to delete data from peer", throwable);
                    }

                    @Override
                    public void onCancel() {

                    }
                });
            }
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * Do publish. If leader, commit publish to store. If not leader, stop publish because should signal to leader.
     *
     * @param datum  datum
     * @param source source raft peer
     * @throws Exception any exception during publish
     */
    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();
        if (datum.value == null) {
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }

        //不是leader的话抛出异常
        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT
                .warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source),
                    JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " + "data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source),
                JacksonUtils.toJson(local));
            throw new IllegalStateException(
                "out of date publish, pub-term:" + source.term.get() + ", cur-term: " + local.term.get());
        }

        //重置一下 .1重置选举时间
        local.resetLeaderDue();

        // if data should be persisted, usually this is true:
        if (KeyBuilder.matchPersistentKey(datum.key)) { //如果是PersistentKey就存储到raftStore中，这个就是往文件中写
            raftStore.write(datum);
        }

        //再存储到内存中
        datums.put(datum.key, datum);

        if (isLeader()) {//如果是leader的话，就将这个本地的term+100
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
        } else {
            //如果不是leader
            //纠正本地的term，本地的加100 大于leader的那个term了，就纠正一下本地的
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                //本地的也加100
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
        }
        //将term写到文件中
        raftStore.updateTerm(local.term.get());
        //发送ValueChangeEvent事件
        //这个onPublish 很重要，因为follower节点收到同步请求之后，也是调用了这个方法，它主要是干了6件事情，
        // 1是重置下选举倒计时，毕竟leader还活着，follower节点收到了leader 同步请求也是执行这个方法，也是需要重置倒计时的。
        //2是，如果是持久节点， 将datum写到文件中。
        //就是这个样子，里面记录了service详细信息与节点集合信息。
        //3是将datum存储到内存中，其实就是put到一个map了。
        //4是更新term值，leader的话+100，然后follower +100的话不超过leader就可以，超过了就使用leader的值。
        //5是将这个term写到文件中，不然的服务突然挂了，再起来就不知道term是多少了，或者是整个集群挂了，重启的话，不知道以谁的数据为准了，因为选举leader就是看的这个term值大小。
        //6是发布服务实例改变事件，发布到通知中心中，然后PersistentNotifier 这个类会订阅这个事件，然后进行通知了，其实这里就是异步通知。
        //好了，onPublish 方法就介绍完了。
        //接着raftCore的signalPublish 方法往下看
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }

    /**
     * Do delete. If leader, commit delete to store. If not leader, stop delete because should signal to leader.
     *
     * @param datumKey datum key
     * @param source   source raft peer
     * @throws Exception any exception during delete
     */
    public void onDelete(String datumKey, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT
                .warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source),
                    JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source),
                JacksonUtils.toJson(local));
            throw new IllegalStateException(
                "out of date publish, pub-term:" + source.term + ", cur-term: " + local.term);
        }

        local.resetLeaderDue();

        // do apply
        String key = datumKey;
        deleteDatum(key);

        if (KeyBuilder.matchServiceMetaKey(key)) {

            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }

            raftStore.updateTerm(local.term.get());
        }

        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);

    }

    @Override
    public void shutdown() throws NacosException {
        this.stopWork = true;
        this.raftStore.shutdown();
        this.peers.shutdown();
        Loggers.RAFT.warn("start to close old raft protocol!!!");
        Loggers.RAFT.warn("stop old raft protocol task for notifier");
        NotifyCenter.deregisterSubscriber(notifier);
        Loggers.RAFT.warn("stop old raft protocol task for master task");
        masterTask.cancel(true);
        Loggers.RAFT.warn("stop old raft protocol task for heartbeat task");
        heartbeatTask.cancel(true);
        Loggers.RAFT.warn("clean old cache datum for old raft");
        datums.clear();
    }

    //选举任务
    //首先是检查下状态，是否准备好等等，接着获取自己的peer，这个peer可以理解成自己的节点信息。拿自己的leaderDueMs 减500，
    // 这个leaderDueMs 一开始是0到15000的随机数，只有减到负数才能继续往下，否则就是return 等待下次的调度（也就是等500ms），
    // 这个随机数说实话是非常重要的，再往下走就是重置的这个leaderDue 与heartBeatDue。leaderDue 重置到15000到20000之间，heartBeatDue就是5000。接着就是调用sendVote方法 发送选票了。
    public class MasterElection implements Runnable {

        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                //判断是否准备好了
                if (!peers.isReady()) {
                    return;
                }

                //获取自己的peer
                RaftPeer local = peers.local();
                //减去 500
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

                //大于0就什么也不干，这玩意早晚有减到小于0的时候
                if (local.leaderDueMs > 0) {
                    return;
                }

                // reset timeout
                //reset timeout 重置leaderDue  15000+ 0到5000的随机数
                local.resetLeaderDue();
                //重置 HeartBeatDue  5000
                local.resetHeartbeatDue();

                //发送选票
                sendVote();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }

        }

        /**
         * 先是获取自己的peer ，然后就是重置这个peer ，其实就是将voteFor这个变量设置成null了，这个变量就是投票给谁的意思
         * ，接着自己的term自己加1，这个也很重要，接着投票给自己，就是voteFor设置成了自己的ip，角色变成candidate 候选人
         * 。最后就是给自己拉票了，将自己这个peer作为参数传到集群所有的节点中。
         * 我们看下其他节点收到拉票请求是怎样处理的，这拉票处理是在RaftCore的receivedVote方法中处理的
         */
        private void sendVote() {
            //获取本机的peer
            RaftPeer local = peers.get(NetUtils.localServer());
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}", JacksonUtils.toJson(getLeader()),
                local.term);

            //将所有的voteFor 都设置为null， 这个voteFor 就是将票投给了谁
            peers.reset();

            //term++ 这个东西还是比较重要的，如果没有这个东西的话，大家都一样，就选不出来了，反正就选那个term比自己大的那个
            local.term.incrementAndGet();
            //先投票给自己
            local.voteFor = local.ip;
            //转变角色为候选人
            local.state = RaftPeer.State.CANDIDATE;

            Map<String, String> params = new HashMap<>(1);
            //封装参数，将自己扔过去了
            params.put("vote", JacksonUtils.toJson(local));
            //遍历那堆抛去自己的server
            for (final String server : peers.allServersWithoutMySelf()) {
                final String url = buildUrl(server, API_VOTE);
                try {
                    HttpClient.asyncHttpPost(url, null, params, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", result.getCode(), url);
                                return;
                            }

                            RaftPeer peer = JacksonUtils.toObj(result.getData(), RaftPeer.class);

                            Loggers.RAFT.info("received approve from peer: {}", JacksonUtils.toJson(peer));

                            peers.decideLeader(peer);

                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("error while sending vote to server: {}", server, throwable);
                        }

                        @Override
                        public void onCancel() {

                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }

    /**
     * Received vote.
     * 处理选票
     *
     * @param remote remote raft peer of vote information
     * @return self-peer information
     */
    //这个方法就是将自己的peer取出来，然后自己的term 与对端的term进行大小比较，如果是对方term比自己的小，就会将voteFor设置成自己，然后把自己的peer回复给拉票方，
    // 其实就是告诉对方你的term太小了，然后我要投票给我自己。
    //如果是对方的term比自己的大，重置leaderDue ，重置leaderDue 后它的选举任务在15000到20000 毫秒之内就不再往下进行了，因为它一次减500，减不到小于0就return
    // 。接着就是自己角色设置成FOLLOWER， 投票给对端的，自己的term重置成对端的term，返回给拉票方。
    public synchronized RaftPeer receivedVote(RaftPeer remote) {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }
        //获取本机的peer
        RaftPeer local = peers.get(NetUtils.localServer());
        //如果远端的peer的term小于等于自己的这个peer的term的话
        if (remote.term.get() <= local.term.get()) {
            String msg = "received illegitimate vote" + ", voter-term:" + remote.term + ", votee-term:" + local.term;

            Loggers.RAFT.info(msg);
            if (StringUtils.isEmpty(local.voteFor)) { //如果voteFOr是null 就设置为自己返回
                local.voteFor = local.ip;
            }

            return local;
        }
        //重置leaderDue
        local.resetLeaderDue();

        //改变状态
        local.state = RaftPeer.State.FOLLOWER;
        //投票给这个ip
        local.voteFor = remote.ip;
        local.term.set(remote.term.get());

        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

        return local;
    }

    /**
     * 发送心跳的任务
     * 跟选举差不多，也是先整上个0-5000让你减，减到小于0 就可以往下走了，接着就是重置这个心跳，重置成5000，最后调用sendBeat方法，发送心跳
     */
    public class HeartBeat implements Runnable {

        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                if (!peers.isReady()) {
                    return;
                }

                RaftPeer local = peers.local();
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
                if (local.heartbeatDueMs > 0) {
                    return;
                }

                local.resetHeartbeatDue();
                //发送心跳
                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }

        }

        /**
         * 如果当前不是集群模式或者自己不是leader ， 就不用再往下执行了，也就是只有集群模式的leader才能发送心跳，接着重置一下自己的leaderDue，
         * 这个也是告诉自己的那个选举任务，在15000到20000之间不要选举了，如果不是仅仅发送心跳的话，就是还要带着数据的key与timestamp
         */
        //发送心跳
        private void sendBeat() throws IOException, InterruptedException {
            RaftPeer local = peers.local();
            //如果不是集群模式或者自己不是leader就不往下执行了
            if (EnvUtil.getStandaloneMode() || local.state != RaftPeer.State.LEADER) {
                return;
            }
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
            }
            //重置LeaderDue
            local.resetLeaderDue();

            // build data
            ObjectNode packet = JacksonUtils.createEmptyJsonNode();
            packet.replace("peer", JacksonUtils.transferToJsonNode(local));

            ArrayNode array = JacksonUtils.createEmptyArrayNode();

            //不仅要发送心跳，还要带着数据的话，默认是false，也就是还得带着数据
            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", switchDomain.isSendBeatOnly());
            }

            if (!switchDomain.isSendBeatOnly()) {
                for (Datum datum : datums.values()) {

                    ObjectNode element = JacksonUtils.createEmptyJsonNode();

                    if (KeyBuilder.matchServiceMetaKey(datum.key)) {
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp.get());

                    array.add(element);
                }
            }

            packet.replace("datums", array);
            // broadcast
            Map<String, String> params = new HashMap<String, String>(1);
            params.put("beat", JacksonUtils.toJson(packet));

            String content = JacksonUtils.toJson(params);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();

            //下面这一大堆就是压缩数据，下面的for循环就是向所有的节点发送心跳（抛去自己），收到回应后更新peer。
            //我们再来看看follower节点是怎样处理心跳的
            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);

            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}", content.length(),
                    compressedContent.length());
            }

            for (final String server : peers.allServersWithoutMySelf()) {
                try {
                    final String url = buildUrl(server, API_BEAT);
                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("send beat to server " + server);
                    }
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}", result.getCode(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return;
                            }

                            peers.update(JacksonUtils.toObj(result.getData(), RaftPeer.class));
                            if (Loggers.RAFT.isDebugEnabled()) {
                                Loggers.RAFT.debug("receive beat response from: {}", url);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server,
                                throwable);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }

                        @Override
                        public void onCancel() {

                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }

        }
    }

    /**
     * Received beat from leader. // TODO split method to multiple smaller method.
     * follower节点是如何处理心跳的
     *
     * @param beat beat information from leader
     * @return self-peer information
     * @throws Exception any exception during handle
     */
    public RaftPeer receivedBeat(JsonNode beat) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        final RaftPeer local = peers.local();
        final RaftPeer remote = new RaftPeer();
        JsonNode peer = beat.get("peer");
        remote.ip = peer.get("ip").asText();
        remote.state = RaftPeer.State.valueOf(peer.get("state").asText());
        remote.term.set(peer.get("term").asLong());
        remote.heartbeatDueMs = peer.get("heartbeatDueMs").asLong();
        remote.leaderDueMs = peer.get("leaderDueMs").asLong();
        remote.voteFor = peer.get("voteFor").asText();

        //远端的那个不是leader的话，抛出异常
        if (remote.state != RaftPeer.State.LEADER) {
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}", remote.state,
                JacksonUtils.toJson(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }

        //本地的term大于远端的也不行
        if (local.term.get() > remote.term.get()) {
            Loggers.RAFT
                .info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}",
                    remote.term.get(), local.term.get(), JacksonUtils.toJson(remote), local.leaderDueMs);
            throw new IllegalArgumentException(
                "out of date beat, beat-from-term: " + remote.term.get() + ", beat-to-term: " + local.term.get());
        }

        //本地如果不是FELLOWER角色的话，设置成这个FOLLOWER
        if (local.state != RaftPeer.State.FOLLOWER) {

            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JacksonUtils.toJson(remote));
            // mk follower
            local.state = RaftPeer.State.FOLLOWER;
            local.voteFor = remote.ip;
        }

        //这个方法很长，我们只看下前半部分，后半部分都是处理那些leader带过来的key信息。首先是取出来leader传过来的数据，判断对端是不是leader，
        // 不是不行，判断对端的term与自己的term ，如果自己的大于的对端的话也不行，如果自己不是follower的话，就设置成follower ，并把自己的票给对端
        // ，重置这个leaderDue与heartBeatDue，其实你会发现，只要leader一直不挂，往follower发送心跳，follower 就一直会重置leaderDue，follower的选举任务就一直不会往下走。
        //我们看下这个makeLeader()方法

        final JsonNode beatDatums = beat.get("datums");
        local.resetLeaderDue();
        local.resetHeartbeatDue();

        peers.makeLeader(remote);

        if (!switchDomain.isSendBeatOnly()) {

            Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());

            for (Map.Entry<String, Datum> entry : datums.entrySet()) {
                receivedKeysMap.put(entry.getKey(), 0);
            }

            // now check datums
            List<String> batch = new ArrayList<>();

            int processedCount = 0;
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT
                    .debug("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
                        beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            }
            for (Object object : beatDatums) {
                processedCount = processedCount + 1;

                JsonNode entry = (JsonNode) object;
                String key = entry.get("key").asText();
                final String datumKey;

                if (KeyBuilder.matchServiceMetaKey(key)) {
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) {
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    // ignore corrupted key:
                    continue;
                }

                long timestamp = entry.get("timestamp").asLong();

                receivedKeysMap.put(datumKey, 1);

                try {
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp
                        && processedCount < beatDatums.size()) {
                        continue;
                    }

                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        batch.add(datumKey);
                    }

                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue;
                    }

                    String keys = StringUtils.join(batch, ",");

                    if (batch.size() <= 0) {
                        continue;
                    }

                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}"
                            + ", datums' size is {}, RaftCore.datums' size is {}", getLeader().ip, batch.size(),
                        processedCount, beatDatums.size(), datums.size());

                    // update datum entry
                    String url = buildUrl(remote.ip, API_GET);
                    Map<String, String> queryParam = new HashMap<>(1);
                    queryParam.put("keys", URLEncoder.encode(keys, "UTF-8"));
                    HttpClient.asyncHttpGet(url, null, queryParam, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                return;
                            }

                            List<JsonNode> datumList = JacksonUtils
                                .toObj(result.getData(), new TypeReference<List<JsonNode>>() {
                                });

                            for (JsonNode datumJson : datumList) {
                                Datum newDatum = null;
                                OPERATE_LOCK.lock();
                                try {

                                    Datum oldDatum = getDatum(datumJson.get("key").asText());

                                    if (oldDatum != null && datumJson.get("timestamp").asLong() <= oldDatum.timestamp
                                        .get()) {
                                        Loggers.RAFT
                                            .info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                                                datumJson.get("key").asText(),
                                                datumJson.get("timestamp").asLong(), oldDatum.timestamp);
                                        continue;
                                    }

                                    if (KeyBuilder.matchServiceMetaKey(datumJson.get("key").asText())) {
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.get("key").asText();
                                        serviceDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        serviceDatum.value = JacksonUtils
                                            .toObj(datumJson.get("value").toString(), Service.class);
                                        newDatum = serviceDatum;
                                    }

                                    if (KeyBuilder.matchInstanceListKey(datumJson.get("key").asText())) {
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.get("key").asText();
                                        instancesDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        instancesDatum.value = JacksonUtils
                                            .toObj(datumJson.get("value").toString(), Instances.class);
                                        newDatum = instancesDatum;
                                    }

                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue;
                                    }

                                    raftStore.write(newDatum);

                                    datums.put(newDatum.key, newDatum);
                                    notifier.notify(newDatum.key, DataOperation.CHANGE, newDatum.value);

                                    local.resetLeaderDue();

                                    if (local.term.get() + 100 > remote.term.get()) {
                                        getLeader().term.set(remote.term.get());
                                        local.term.set(getLeader().term.get());
                                    } else {
                                        local.term.addAndGet(100);
                                    }

                                    raftStore.updateTerm(local.term.get());

                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}",
                                        newDatum.key, newDatum.timestamp, JacksonUtils.toJson(remote), local.term);

                                } catch (Throwable e) {
                                    Loggers.RAFT
                                        .error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum,
                                            e);
                                } finally {
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                            } catch (InterruptedException e) {
                                Loggers.RAFT.error("[RAFT-BEAT] Interrupted error ", e);
                            }
                            return;
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader", throwable);
                        }

                        @Override
                        public void onCancel() {

                        }

                    });

                    batch.clear();

                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }

            }

            List<String> deadKeys = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) {
                    deadKeys.add(entry.getKey());
                }
            }

            for (String deadKey : deadKeys) {
                try {
                    deleteDatum(deadKey);
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }

        }

        return local;
    }

    /**
     * Add listener for target key.
     *
     * @param key      key
     * @param listener new listener
     */
    public void listen(String key, RecordListener listener) {
        notifier.registerListener(key, listener);

        Loggers.RAFT.info("add listener: {}", key);
        // if data present, notify immediately
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }

            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }

    /**
     * Remove listener for key.
     *
     * @param key      key
     * @param listener listener
     */
    public void unListen(String key, RecordListener listener) {
        notifier.deregisterListener(key, listener);
    }

    public void unListenAll(String key) {
        notifier.deregisterAllListener(key);
    }

    public void setTerm(long term) {
        peers.setTerm(term);
    }

    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }

    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }

    /**
     * Build api url.
     *
     * @param ip  ip of api
     * @param api api path
     * @return api url
     */
    public static String buildUrl(String ip, String api) {
        if (!IPUtil.containsPort(ip)) {
            ip = ip + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort();
        }
        return "http://" + ip + EnvUtil.getContextPath() + api;
    }

    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }

    public RaftPeer getLeader() {
        return peers.getLeader();
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }

    public RaftPeerSet getPeerSet() {
        return peers;
    }

    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }

    public int datumSize() {
        return datums.size();
    }

    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
    }

    /**
     * Load datum.
     *
     * @param key datum key
     */
    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }

    }

    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) {
                raftStore.delete(deleted);
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            NotifyCenter.publishEvent(
                ValueChangeEvent.builder().key(URLDecoder.decode(key, "UTF-8")).action(DataOperation.DELETE)
                    .build());
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    @Deprecated
    public int getNotifyTaskCount() {
        return (int) publisher.currentEventSize();
    }

}
