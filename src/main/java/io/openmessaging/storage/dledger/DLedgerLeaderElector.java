/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    /**
     * 随机数生成器，对应 raft 协议中选举超时时间是一随机数
     */
    private Random random = new Random();
    /**
     * 配置参数
     */
    private DLedgerConfig dLedgerConfig;
    /**
     * 节点状态机
     */
    private final MemberState memberState;
    /**
     * rpc 服务，实现向集群内的节点发送心跳包、投票的 RPC 实现。
     */
    private DLedgerRpcService dLedgerRpcService;

    /**
     * 上次收到心跳包的时间戳
     */
    private long lastLeaderHeartBeatTime = -1;
    /**
     * 上次发送心跳包的时间戳
     */
    private long lastSendHeartBeatTime = -1;
    /**
     * 上次成功收到心跳包的时间戳
     */
    private long lastSuccHeartBeatTime = -1;
    /**
     * 一个心跳包的周期，默认为 2s
     */
    private int heartBeatTimeIntervalMs = 2000;
    /**
     * 允许最大的 N 个心跳周期内未收到心跳包，状态为 Follower 的节点只有超过
     * maxHeartBeatLeak * heartBeatTimeIntervalMs 的时间内未收到主节点的心跳包， 才会重新进入 Candidate 状态，重新下一轮的选举。
     */
    private int maxHeartBeatLeak = 3;
    //as a client

    /**
     * 发送下一个心跳包的时间戳
     */
    private long nextTimeToRequestVote = -1;
    /**
     * 是否应该立即发起投票
     */
    private boolean needIncreaseTermImmediately = false;
    /**
     * 最小的发送投票间隔时间，默认为 300ms
     */
    private int minVoteIntervalMs = 300;
    /**
     * 最大的发送投票的间隔，默认为 1000ms
     */
    private int maxVoteIntervalMs = 1000;

    /**
     * 注册的节点状态处理器，通过 addRoleChangeHandler 方法添加
     */
    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    /**
     * 上一次的投票结果, 默认是 WAIT_TO_REVOTE
     */
    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
    /**
     * 上一次投票的开销
     */
    private long lastVoteCost = 0L;
    /**
     * 状态机管理器
     */
    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        // 启动状态维护管理器
        stateMaintainer.start();
        // 遍历状态改变监听器并启动它，可通过 DLedgerLeaderElector 的 addRoleChangeHandler 方法增加状态变化监听器。
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {

        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        if (request.getTerm() < memberState.currTerm()) {
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) {
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.getLeaderId() == null) {
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.currVoteFor() == null) {
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                } else {
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY__HAS_LEADER));
                    } else {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            //assert acceptedTerm is true
            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    private void sendHeartbeats(long term, String leaderId) throws Exception {
        final AtomicInteger allNum = new AtomicInteger(1);
        final AtomicInteger succNum = new AtomicInteger(1);
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        final AtomicLong maxTerm = new AtomicLong(-1);
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        final CountDownLatch beatLatch = new CountDownLatch(1);
        long startHeartbeatTimeMs = System.currentTimeMillis();
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {

                    if (ex != null) {
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }
                    if (memberState.isQuorum(succNum.get())
                        || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Parse heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        beatLatch.countDown();
                    }
                }
            });
        }
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        if (memberState.isQuorum(succNum.get())) {
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) {
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                changeRoleToCandidate(term);
            }
        }
    }

    private void maintainAsLeader() throws Exception {
        // 首先判断上一次发送心跳的时间与当前时间的差值是否大于心跳包发送间隔，如果超过，则说明需要发送心跳包
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                // 如果当前不是 leader 节点，则直接返回，主要是为了二次判断
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            // 向集群内的所有节点发送心跳包
            sendHeartbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        // 如果 maxHeartBeatLeak (默认为 3)个心跳包周期内未收到心跳，则将状态变更为 Candidate
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm,
        long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                voteResponse = handleVote(voteRequest, true);
            } else {
                //async
                voteResponse = dLedgerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    private long getNextTimeToRequestVote() {
        return System.currentTimeMillis() + lastVoteCost + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    private void maintainAsCandidate() throws Exception {
        // for candidate
        // nextTimeToRequestVote: 下一次发发起的投票的时间，如果当前时间小于该值，说明计时器未过期，此时无需发起投票。
        // needIncreaseTermImmediately: 是否应该立即发起投票。如果为 true，则忽略计时器，该值默认为 false，
        // 当收到从主节点的心跳包并且当前状态机的轮次大于主节点的轮次，说明集群中 Leader 的投票轮次小于从几点的轮次，应该立即发起新的投票
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        // 投票轮次
        long term;
        // Leader 节点当前的投票轮次
        long ledgerEndTerm;
        // 当前日志的最大序列，即下一条日志的开始 index，在日志复制部分会详细介绍
        long ledgerEndIndex;
        synchronized (memberState) {
            if (!memberState.isCandidate()) {
                return;
            }
            // 第一次 lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT 返回 true
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.currTerm();
                // 第一轮都是 -1
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                term = memberState.currTerm();
            }
            // 第一轮都是 -1
            ledgerEndIndex = memberState.getLedgerEndIndex();
            // 第一轮都是 -1
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }
        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();
        // 向集群内的其他节点发起投票请，并返回投票结果列表
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        // 已知的最大投票轮次
        final AtomicLong knownMaxTermInGroup = new AtomicLong(-1);
        // 所有投票票数
        final AtomicInteger allNum = new AtomicInteger(0);
        // 有效投票数
        final AtomicInteger validNum = new AtomicInteger(0);
        // 获得的投票数
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        // 未准备投票的节点数量，如果对端节点的投票轮次小于发起投票的轮次，则认为对端未准备好，对端节点使用本次的轮次进入 Candidate 状态
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        // 发起投票的节点的 ledgerEndTerm 小于对端节点的个数
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0);
        // 是否已经存在 Leader
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        CountDownLatch voteLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    // 如果投票结果不是 UNKNOW，则有效投票数量增 1
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            // 赞成票，acceptedNum 加一，只有得到的赞成票超过集群节点数量的一半才能成为Leader
                            case ACCEPT:
                                acceptedNum.incrementAndGet();
                                break;
                            // 拒绝票，原因是已经投了其他节点的票
                            case REJECT_ALREADY_VOTED:
                                break;
                            // 拒绝票，原因是因为集群中已经存在 LEADER 了
                            // alreadyHasLeader 设置为 true，无
                            // 需在判断其他投票结果了，结束本轮投票
                            case REJECT_ALREADY__HAS_LEADER:
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            // 拒绝票，如果自己维护的 term 小于远端维护的 ledgerEndTerm，则返回该结果
                            // 如果对端的 team 大于自己的 team，需要记录对端最大的投票轮次，以便更新自己的投票轮次。
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            // 拒绝票，如果自己维护的 term 小于远端维护的 term，更新自己维护的投票轮次
                            case REJECT_EXPIRED_VOTE_TERM:
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            // 拒绝票，如果自己维护的 ledgerTerm 小于对端维护的 ledgerTerm，则返回该结果。
                            // 如果是此种情况，增加计数器 biggerLedgerNum 的值。
                            case REJECT_EXPIRED_LEDGER_TERM:
                            // 拒绝票，如果对端的 ledgerTeam 与自己维护的 ledgerTeam 相等，但是自己维护的
                            // dLedgerEndIndex 小于对端维护的值，返回该值，增加 biggerLedgerNum 计数器的值。
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                biggerLedgerNum.incrementAndGet();
                                break;
                            // 拒绝票，对端的投票轮次小于自己的 team，则认为对端还未准备好投票，对端使用自
                            // 己的投票轮次，是自己进入到 Candidate 状态。
                            case REJECT_TERM_NOT_READY:
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;
                        }
                    }

                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Get error when parsing vote response ", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            }); // end of whenComplete
        // end of iter
        }
        try {
            // 等待收集投票结果，并设置超时时间
            voteLatch.await(3000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }
        // 根据收集的投票结果判断是否能成为 Leader
        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        if (knownMaxTermInGroup.get() > term) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (memberState.isQuorum(acceptedNum.get())) {
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else if (memberState.isQuorum(acceptedNum.get() + biggerLedgerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }

        lastParseResult = parseResult;
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
            memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        // 如果投票成功，则状态机状态设置为 Leader，然后状态管理在驱动状态时
        // 会调用 DLedgerLeaderElector#maintainState 时，将进入到 maintainAsLeader 方法
        if (parseResult == VoteResponse.ParseResult.PASSED) {
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
        }

    }

    /**
     * The core method of maintainer.
     * Run the specified logic according to the current role:
     *  candidate => propose a vote.
     *  leader => send heartbeats to followers, and step down to candidate when quorum followers do not respond.
     *  follower => accept heartbeats, and change to candidate when no heartbeat from leader.
     * @throws Exception
     */
    private void maintainState() throws Exception {
        // private Role role = CANDIDATE;
        // 发现其初始状态为 CANDIDATE
        if (memberState.isLeader()) {
            // 领导者，主节点，该状态下，需要定时向从节点发送心跳包，用来传播数据、确保其领导地位。
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            // 从节点，该状态下，会开启定时器，尝试进入到 candidate 状态，以便发起投票选举，同时一旦收到主节点的心跳包，则重置定时器。
            maintainAsFollower();
        } else {
            // 候选者，该状态下的节点会发起投票，尝试选择自己为主节点，选举成功后，不会存在该状态下的节点。
            maintainAsCandidate();
        }
    }

    private void handleRoleChange(long term, MemberState.Role role) {
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                // 如果该节点参与 Leader 选举
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {
                    // 刷新配置
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);
                    // 开始选举
                    DLedgerLeaderElector.this.maintainState();
                }
                // 没执行一次选主，休息 10ms
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

}
