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

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sets of raft peers.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@Component
@DependsOn("ProtocolManager")
public class RaftPeerSet extends MemberChangeListener implements Closeable {

    private final ServerMemberManager memberManager;

    private AtomicLong localTerm = new AtomicLong(0L);

    private RaftPeer leader = null;

    private volatile Map<String, RaftPeer> peers = new HashMap<>(8);

    private Set<String> sites = new HashSet<>();

    private volatile boolean ready = false;

    private Set<Member> oldMembers = new HashSet<>();

    public RaftPeerSet(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
    }

    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscriber(this);
        changePeers(memberManager.allMembers());
    }

    @Override
    public void shutdown() throws NacosException {
        this.localTerm.set(-1);
        this.leader = null;
        this.peers.clear();
        this.sites.clear();
        this.ready = false;
        this.oldMembers.clear();
    }

    public RaftPeer getLeader() {
        if (EnvUtil.getStandaloneMode()) {
            return local();
        }
        return leader;
    }

    public Set<String> allSites() {
        return sites;
    }

    public boolean isReady() {
        return ready;
    }

    /**
     * Remove raft node.
     *
     * @param servers node address need to be removed
     */
    public void remove(List<String> servers) {
        for (String server : servers) {
            peers.remove(server);
        }
    }

    /**
     * Update raft peer.
     *
     * @param peer new peer.
     * @return new peer
     */
    public RaftPeer update(RaftPeer peer) {
        peers.put(peer.ip, peer);
        return peer;
    }

    /**
     * Judge whether input address is leader.
     *
     * @param ip peer address
     * @return true if is leader or stand alone, otherwise false
     */
    public boolean isLeader(String ip) {
        if (EnvUtil.getStandaloneMode()) {
            return true;
        }

        if (leader == null) {
            Loggers.RAFT.warn("[IS LEADER] no leader is available now!");
            return false;
        }

        return StringUtils.equals(leader.ip, ip);
    }

    public Set<String> allServersIncludeMyself() {
        return peers.keySet();
    }

    /**
     * Get all servers excludes current peer.
     *
     * @return all servers excludes current peer
     */
    public Set<String> allServersWithoutMySelf() {
        Set<String> servers = new HashSet<String>(peers.keySet());

        // exclude myself
        servers.remove(local().ip);

        return servers;
    }

    public Collection<RaftPeer> allPeers() {
        return peers.values();
    }

    public int size() {
        return peers.size();
    }

    /**
     * Calculate and decide which peer is leader. If has new peer has more than half vote, change leader to new peer.
     * 拉票方收到接收方的选票
     * 这个计算自己多少票，看看谁能当leader的方法了，把收到的投票信息放到peers中，遍历找出最大票数与对应的那个peer，如果是最大票数超过了半数+1
     * @param candidate new candidate
     * @return new leader if new candidate has more than half vote, otherwise old leader
     */
    //就会从peer中将取出那个当选的peer ，将这个peer设置成leader，如果自己本地的leader不是这个新当选的leader ，就会替换下，并发布leader选举完成的事件。
    //好了，到这leader选举就完成了，关键点就是在这个term上，term越大越能当选，如果是一开始term相等的话，那个leaderDue是非常重要的，15000到20000 这之间的随机数，
    // 如果是15000的就比20000的早执行选举，term也就早+1 ，早发起拉票，早当选。

    public RaftPeer decideLeader(RaftPeer candidate) {
        peers.put(candidate.ip, candidate);

        //统计出现次数的一个bag
        SortedBag ips = new TreeBag();
        int maxApproveCount = 0; //最多票
        String maxApprovePeer = null; //最多票的peer
        for (RaftPeer peer : peers.values()) {
            if (StringUtils.isEmpty(peer.voteFor)) {
                continue;
            }

            ips.add(peer.voteFor);
            if (ips.getCount(peer.voteFor) > maxApproveCount) {
                maxApproveCount = ips.getCount(peer.voteFor);
                maxApprovePeer = peer.voteFor;
            }
        }

        //如果超过半数+1的话
        if (maxApproveCount >= majorityCount()) {
            //获取最大票数的peer
            RaftPeer peer = peers.get(maxApprovePeer);
            //然后将peer设置为leader
            peer.state = RaftPeer.State.LEADER;

            //如果leader不是选出来的那个peer，就把leader设置为选出来的那个peer
            if (!Objects.equals(leader, peer)) {
                leader = peer;
                //发布leader选举完成的事件
                ApplicationUtils.publishEvent(new LeaderElectFinishedEvent(this, leader, local()));
                Loggers.RAFT.info("{} has become the LEADER", leader.ip);
            }
        }

        return leader;
    }

    /**
     * Set leader as new candidate.
     *
     * @param candidate new candidate
     * @return new leader
     */
    //这个方法主要就是更新下自己本地维护的leader，如果本地维护的leader不是远端那个的话，就重新设置下本地的leader，并且发布leader改变的事件，
    //接着就是遍历所有的peer，找出以前那个leader ，发送请求，获取一下它的peer信息，然后更新下维护的它的peer信息，最后更新下本地委会的leader peer 信息。
    //好了，到这我们心跳这块也是ok了，心跳执行leader向其他的节点发送，让其他节点知道leader 还活着，如果leader长时间不发送，follower就认为leader挂了，
    // 继续走选举的逻辑，这里重置leaderDue是非常重要的。
    public RaftPeer makeLeader(RaftPeer candidate) {
        if (!Objects.equals(leader, candidate)) { //如果leader不是，就远端就设置成这个。
            leader = candidate;
            //通知MakeLeaderEvent事件
            ApplicationUtils.publishEvent(new MakeLeaderEvent(this, leader, local()));
            Loggers.RAFT
                    .info("{} has become the LEADER, local: {}, leader: {}", leader.ip, JacksonUtils.toJson(local()),
                            JacksonUtils.toJson(leader));
        }

        for (final RaftPeer peer : peers.values()) {
            Map<String, String> params = new HashMap<>(1);
            //peer不是candidate，然后peer是leader
            if (!Objects.equals(peer, candidate) && peer.state == RaftPeer.State.LEADER) {
                try {
                    // /v1/ns/raft/peer
                    String url = RaftCore.buildUrl(peer.ip, RaftCore.API_GET_PEER);
                    HttpClient.asyncHttpGet(url, null, params, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT
                                        .error("[NACOS-RAFT] get peer failed: {}, peer: {}", result.getCode(), peer.ip);
                                peer.state = RaftPeer.State.FOLLOWER;
                                return;
                            }
                            //更新下原先是leader的peer
                            update(JacksonUtils.toObj(result.getData(), RaftPeer.class));
                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onCancel() {

                        }
                    });
                } catch (Exception e) {
                    peer.state = RaftPeer.State.FOLLOWER;
                    Loggers.RAFT.error("[NACOS-RAFT] error while getting peer from peer: {}", peer.ip);
                }
            }
        }

        return update(candidate);
    }

    /**
     * Get local raft peer.
     *
     * @return local raft peer
     */
    public RaftPeer local() {
        RaftPeer peer = peers.get(EnvUtil.getLocalAddress());
        if (peer == null && EnvUtil.getStandaloneMode()) {
            RaftPeer localPeer = new RaftPeer();
            localPeer.ip = NetUtils.localServer();
            localPeer.term.set(localTerm.get());
            peers.put(localPeer.ip, localPeer);
            return localPeer;
        }
        if (peer == null) {
            throw new IllegalStateException(
                    "unable to find local peer: " + NetUtils.localServer() + ", all peers: " + Arrays
                            .toString(peers.keySet().toArray()));
        }

        return peer;
    }

    public RaftPeer get(String server) {
        return peers.get(server);
    }

    //半数+1
    public int majorityCount() {
        return peers.size() / 2 + 1;
    }

    /**
     * Reset set.
     */
    public void reset() {

        leader = null;

        for (RaftPeer peer : peers.values()) {
            peer.voteFor = null;
        }
    }

    public void setTerm(long term) {
        localTerm.set(term);
    }

    public long getTerm() {
        return localTerm.get();
    }

    public boolean contains(RaftPeer remote) {
        return peers.containsKey(remote.ip);
    }

    @Override
    public void onEvent(MembersChangeEvent event) {
        Collection<Member> members = event.getMembers();
        Collection<Member> newMembers = new HashSet<>(members);
        newMembers.removeAll(oldMembers);

        // If an IP change occurs, the change starts
        if (!newMembers.isEmpty()) {
            changePeers(members);
        }

        oldMembers.clear();
        oldMembers.addAll(members);
    }

    protected void changePeers(Collection<Member> members) {
        Map<String, RaftPeer> tmpPeers = new HashMap<>(members.size());

        for (Member member : members) {

            final String address = member.getAddress();
            if (peers.containsKey(address)) {
                tmpPeers.put(address, peers.get(address));
                continue;
            }

            RaftPeer raftPeer = new RaftPeer();
            raftPeer.ip = address;

            // first time meet the local server:
            if (EnvUtil.getLocalAddress().equals(address)) {
                raftPeer.term.set(localTerm.get());
            }

            tmpPeers.put(address, raftPeer);
        }

        // replace raft peer set:
        peers = tmpPeers;

        ready = true;
        Loggers.RAFT.info("raft peers changed: " + members);
    }

    @Override
    public String toString() {
        return "RaftPeerSet{" + "localTerm=" + localTerm + ", leader=" + leader + ", peers=" + peers + ", sites="
                + sites + '}';
    }
}