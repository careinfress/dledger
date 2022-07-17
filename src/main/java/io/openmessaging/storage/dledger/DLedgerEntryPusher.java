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
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerEntryPusher {

    private static Logger logger = LoggerFactory.getLogger(DLedgerEntryPusher.class);
    // 多副本相关配置
    private DLedgerConfig dLedgerConfig;
    // 存储实现类
    private DLedgerStore dLedgerStore;
    // 节点状态机
    private final MemberState memberState;
    // RPC 服务实现类，用于集群内的其他节点进行网络通讯
    private DLedgerRpcService dLedgerRpcService;
    // 每个节点基于投票轮次的当前水位线标记。键值为投票轮次，值为 ConcurrentMap <节点id/, Long/ 节点对应的日志序号/>
    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();
    // 用于存放追加请求的响应结果(Future模式)
    private Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    // 日志接收处理线程，当节点为从节点时激活
    // 从节点上开启的线程，用于接收主节点的 push 请求（append、commit、append）
    private EntryHandler entryHandler = new EntryHandler(logger);
    // 日志追加ACK投票处理线程，当前节点为主节点时激活
    // 主节点上的追加请求投票器
    private QuorumAckChecker quorumAckChecker = new QuorumAckChecker(logger);
    // 主节点日志请求转发器，向从节点复制消息等
    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        // 构建 DLedgerEntryPusher 时会为每一个子节点创建一 EntryDispatcher 对象
        // 这里类似 zk 为一个 Follower 创建一个 leanerHandel 对象
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger));
            }
        }
    }

    public void startup() {
        // 日志接收处理线程，当节点为从节点时激活
        entryHandler.start();
        // 日志追加ACK投票处理线程，当前节点为主节点时激活
        quorumAckChecker.start();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            // 日志转发线程，当前节点为主节点时追加
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            logger.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            logger.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    /**
     *
     * @param term 当前的投票轮次
     * @param peerId 当前节点的 ID。
     * @param index 当前追加数据的序号
     */
    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            // 初始化 peerWaterMarksByTerm 数据结构
            // 其结果为 < Long /** term */, Map<String /** peerId */, Long /** entry index*/>
            checkTermForWaterMark(term, "updatePeerWaterMark");
            // 如果 peerWaterMarksByTerm 存储的 index 小于当前数据的 index， 则更新
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    private long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    /**
     * 如何判断 Push 队列是否已满
     * @param currTerm
     * @return
     */
    public boolean isPendingFull(long currTerm) {
        // 检查当前投票轮次是否在 PendingMap 中，如果不在，则初始化，其结构为
        // Map<Long/* 投票轮次*/, ConcurrentMap<Long, TimeoutFuture<AppendEnt ryResponse>>>
        checkTermForPendingMap(currTerm, "isPendingFull");
        // 检测当前等待从节点返回结果的个数是否超过其最大请求数量，可通过 maxPendingRequestsNum 配置，该值默认为:10000。
        return pendingAppendResponsesByTerm.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    /**
     * 主节点等待从节点复制 ACK
     * @param entry
     * @return
     */
    public CompletableFuture<AppendEntryResponse> waitAck(DLedgerEntry entry) {
        // 更新当前节点的 push 水位线
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        // 如果集群的节点个数为 1，无需转发，直接返回成功结果
        if (memberState.getPeerMap().size() == 1) {
            AppendEntryResponse response = new AppendEntryResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getSelfId());
            response.setIndex(entry.getIndex());
            response.setTerm(entry.getTerm());
            response.setPos(entry.getPos());
            return AppendFuture.newCompletedFuture(entry.getPos(), response);
        }
        // 构建 append 响应 Future 并设置超时时间，默认值为:2500 ms，可以通过 maxWaitAckTimeMs 配置改变其默认值
        else {
            checkTermForPendingMap(entry.getTerm(), "waitAck");
            AppendFuture<AppendEntryResponse> future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            future.setPos(entry.getPos());
            // 将构建的 Future 放入等待结果集合中
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                logger.warn("[MONITOR] get old wait at index={}", entry.getIndex());
            }
            // 唤醒 Entry 转发线程，即将主节点中的数据 push 到各个从节点
            wakeUpDispatchers();
            return future;
        }
    }

    public void wakeUpDispatchers() {
        // 该方法主要就是遍历转发器并唤醒。本方法的核心关键就是 EntryDispatcher
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            // 分发给各个 follow 节点
            dispatcher.wakeup();
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {
        // 上次打印水位线的时间戳，单位为毫秒
        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        // 上次检测泄漏的时间戳，单位为毫秒
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        // 已投票仲裁的日志序号
        private long lastQuorumIndex = -1;

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker", logger);
        }

        @Override
        public void doWork() {
            try {
                /**
                 * Step1：如果离上一次打印 watermak 的时间超过3s，
                 * 则打印一下当前的 term、ledgerBegin、ledgerEnd、committed、peerWaterMarksByTerm 这些数据日志
                 */
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                /**
                 * Step2：如果当前节点不是主节点，直接返回，不作为
                 */
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");

                /**
                 * Step3：清理pendingAppendResponsesByTerm、peerWaterMarksByTerm 中非本次投票轮次的数据，避免一些不必要的内存使用。
                 */
                if (pendingAppendResponsesByTerm.size() > 1) {
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : pendingAppendResponsesByTerm.get(term).entrySet()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setIndex(futureEntry.getKey());
                            response.setCode(DLedgerResponseCode.TERM_CHANGED.getCode());
                            response.setLeaderId(memberState.getLeaderId());
                            logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().complete(response);
                        }
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }
                /**
                 * Step4：根据各个从节点反馈的进度，进行仲裁，确定已提交序号。
                 * 为了加深对这段代码的理解，再来啰嗦一下 peerWaterMarks 的作用，存储的是各个从节点当前已成功追加的日志序号。
                 * 例如一个三节点的 DLedger 集群，peerWaterMarks 数据存储大概如下：
                 * {
                 * “dledger_group_01_0” : 100,
                 * "dledger_group_01_1" : 101,
                 * }
                 * 其中 dledger_group_01_0 为从节点1的ID，当前已复制的序号为 100，
                 * 而 dledger_group_01_1 为节点2的ID，当前已复制的序号为 101。
                 * 再加上主节点，如何确定可提交序号呢？
                 */
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                long quorumIndex = -1;
                // 首先遍历 peerWaterMarks 的 value 集合，即上述示例中的 {100, 101}，
                // 用临时变量 index 来表示待投票的日志序号，需要集群内超过半数的节点的已复制序号超过该值，
                // 则该日志能被确认提交。
                for (Long index : peerWaterMarks.values()) {
                    int num = 0;
                    // 遍历 peerWaterMarks 中的所有已提交序号，
                    // 与当前值进行比较，如果节点的已提交序号大于等于待投票的日志序号(index)，num 加一
                    // 表示投赞成票
                    for (Long another : peerWaterMarks.values()) {
                        if (another >= index) {
                            num++;
                        }
                    }
                    // 对 index 进行仲裁，如果超过半数 并且 index 大于 quorumIndex，
                    // 更新 quorumIndex 的值为 index。
                    // quorumIndex 经过遍历的，得出当前最大的可提交日志序号。
                    if (memberState.isQuorum(num) && index > quorumIndex) {
                        quorumIndex = index;
                    }
                }
                // 更新 committedIndex 索引，方便 DLedgerStore 定时将 committedIndex 写入 checkpoint 中
                // 这个是 commit 的 index
                dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);
                /**
                 * Step5：处理 quorumIndex 之前的挂起请求，需要发送响应到客户端,其实现步骤：
                 */
                ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                boolean needCheck = false;
                int ackNum = 0;
                if (quorumIndex >= 0) {
                    // 从 quorumIndex 开始处理，每处理一条，该序号减一，直到大于0或主动退出，请看后面的退出逻辑。
                    for (Long i = quorumIndex; i >= 0; i--) {
                        try {
                            // responses 中移除该日志条目的挂起请求。
                            CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                            // 如果未找到挂起请求，说明前面挂起的请求已经全部处理完毕，准备退出，退出之前再 设置 needCheck 的值，其依据如下(三个条件必须同时满足)：
                            // 1. 最后一次仲裁的日志序号不等于-1
                            // 2. 并且最后一次不等于本次新仲裁的日志序号
                            // 3. 最后一次仲裁的日志序号不等于最后一次仲裁的日志。正常情况一下，条件一、条件二通常为true，但这一条大概率会返回false
                            if (future == null) {
                                needCheck = lastQuorumIndex != -1 && lastQuorumIndex != quorumIndex && i != lastQuorumIndex;
                                break;
                            }
                            // 向客户端返回结果
                            else if (!future.isDone()) {
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setTerm(currTerm);
                                response.setIndex(i);
                                response.setLeaderId(memberState.getSelfId());
                                response.setPos(((AppendFuture) future).getPos());
                                // 触发客户端的 wait 方法能有所返回
                                future.complete(response);
                            }
                            // ackNum，表示本次确认的数量
                            ackNum++;
                        } catch (Throwable t) {
                            logger.error("Error in ack to index={} term={}", i, currTerm, t);
                        }
                    }
                }
                /**
                 * Step6：如果本次确认的个数为0，
                 * 则尝试去判断超过该仲裁序号的请求，是否已经超时，如果已超时，则返回超时响应结果
                 */
                if (ackNum == 0) {
                    for (long i = quorumIndex + 1; i < Integer.MAX_VALUE; i++) {
                        TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                        if (future == null) {
                            break;
                        } else if (future.isTimeOut()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setCode(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            // 超时了，给客户端触发执行
                            future.complete(response);
                        } else {
                            break;
                        }
                    }
                    // 阻塞1ms
                    waitForRunning(1);
                }
                /**
                 * Step7：检查是否发送泄漏。其判断泄漏的依据是如果挂起的请求的日志序号小于已提交的序号，则移除。
                 */
                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                    updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                    for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
                        if (futureEntry.getKey() < quorumIndex) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(futureEntry.getKey());
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                            futureEntry.getValue().complete(response);
                            responses.remove(futureEntry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                /**
                 * 一次日志仲裁就结束了，最后更新 lastQuorumIndex 为本次仲裁的的新的提交值。
                 */
                lastQuorumIndex = quorumIndex;
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     *   APPEND : append the entries to the follower
     *   COMPARE : if the leader changes, the new leader should compare its entries to follower's
     *   TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     *   COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     *           the leader will send a pure request to inform the follower of committed index.
     *
     *   The common transferring between these types are as following:
     *
     *   COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     *   ^                             |
     *   |---<-----<------<-------<----|
     *
     */
    private class EntryDispatcher extends ShutdownAbleThread {
        // 向从节点发送命令的类型，可选值：PushEntryRequest.Type.COMPARE、TRUNCATE、APPEND、COMMIT，下面详细说明。
        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        // 上一次发送提交类型的时间戳
        private long lastPushCommitTimeMs = -1;
        // 目标节点ID
        private String peerId;
        // 已完成比较的日志序号
        private long compareIndex = -1;
        // 已写入的日志序号
        private long writeIndex = -1;
        // 允许的最大挂起日志数量
        private int maxPendingSize = 1000;
        // Leader 节点当前的投票轮次
        private long term = -1;
        // Leader 节点ID
        private String leaderId = null;
        // 上次检测泄漏的时间，所谓的泄漏，就是看挂起的日志请求数量是否查过了 maxPendingSize
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        // 记录日志的挂起时间，key：日志的序列(entryIndex)，value：挂起时间戳
        private ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();
        // 配额
        private Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        private boolean checkAndFreshState() {
            // 如果节点的状态不是主节点，则直接返回 false。则结束 本次 doWork 方法。因为只有主节点才需要向从节点转发日志。
            if (!memberState.isLeader()) {
                return false;
            }
            /**
             * 如果当前节点状态是主节点，但当前的投票轮次与状态机轮次或 leaderId 还未设置
             * 或 leaderId 与状态机的 leaderId 不相等
             * 这种情况通常是集群触发了重新选举
             * 设置其 term、leaderId 与状态机同步，即将发送 COMPARE 请求。
             */
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return false;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }

        private PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            /**
             * 提交包请求字段主要包含如下字段：
             * DLedger 节点所属组、
             * 从节点 id、
             * 主节点 id，
             * 当前投票轮次、
             * 日志内容、
             * 请求类型
             * committedIndex(主节点已提交日志序号)
             */
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(target);
            // 主节点已提交日志序号
            request.setCommitIndex(dLedgerStore.getCommittedIndex());
            return request;
        }

        private void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            if (dLedgerStore instanceof DLedgerMemoryStore) {
                return;
            }
            DLedgerMmapFileStore mmapFileStore = (DLedgerMmapFileStore) dLedgerStore;
            if (mmapFileStore.getDataFileList().getMaxWrotePosition() - entry.getPos() < dLedgerConfig.getPeerPushThrottlePoint()) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                DLedgerUtils.sleep(quota.leftNow());
            }
        }

        private void doAppendInner(long index) throws Exception {
            // 首先根据序号查询出日志
            DLedgerEntry entry = dLedgerStore.get(index);
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            /**
             * 检测配额，如果超过配额，会进行一定的限流，其关键实现点
             * 1. 首先触发条件：append 挂起请求数已超过最大允许挂起数；基于文件存储并主从差异超过300m，可通过 peerPushThrottlePoint 配置。
             * 2. 每秒追加的日志超过 20m(可通过 peerPushQuota 配置)，则会 sleep 1s中后再追加
             */
            checkQuotaAndWait(entry);
            // 构建 PUSH 请求日志
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            // 通过 Netty 发送网络请求到从节点，从节点收到请求会进行处理
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
            // 用 pendingMap 记录待追加的日志的发送时间，用于发送端判断是否超时的一个依据
            pendingMap.put(index, System.currentTimeMillis());
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        /**
                         * 请求成功的处理逻辑，其关键实现点如下
                         * 1. 移除 pendingMap 中的关于该日志的发送超时时间
                         * 2. 更新已成功追加的日志序号(按投票轮次组织，并且每个从服务器一个键值对)
                         * 3. 唤醒 quorumAckChecker 线程(主要用于仲裁 append 结果)
                         */
                        case SUCCESS:
                            pendingMap.remove(x.getIndex());
                            // 更新从节点当前已经复制的水位。不是commit, 只是刷盘
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            quorumAckChecker.wakeup();
                            break;
                        /**
                         * Push 请求出现状态不一致情况，将发送 COMPARE 请求，来对比主从节点的数据是否一致
                         */
                         case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            // 如果 follow 返回不是 success 则进行数据校正
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        private void doCommit() throws Exception {
            // 如果上一次单独发送 commit 的请求时间与当前时间相隔低于 1s，放弃本次提交请求
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                // 构建提交请求
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                // Ignore the results
                // 通过网络向从节点发送 commit 请求
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        /**
         * 该方法的作用是检查 append 请求是否超时
         * @throws Exception
         */
        private void doCheckAppendResponse() throws Exception {
            // 获取已成功 append 的序号
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            /**
             * 从挂起的请求队列中获取下一条的发送时间，如果不为空并去超过了 append 的超时时间
             * 则再重新发送 append 请求，最大超时时间默认为 1s，可以通过 maxPushTimeOutMs 来改变默认值
             */
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }

        private void doAppend() throws Exception {
            while (true) {
                // 检查状态
                if (!checkAndFreshState()) {
                    break;
                }
                // 如果请求类型不为 APPEND，则退出，结束本轮 doWork 方法执行
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                /**
                 * writeIndex 表示当前追加到从该节点的序号
                 * 通常情况下主节点向从节点发送 append 请求时，会附带主节点的已提交指针
                 * 但如果 append 请求发不那么频繁，writeIndex 大于 leaderEndIndex 时
                 * （由于 pending 请求超过其 pending 请求的队列长度（默认为1w)，时
                 * 会阻止数据的追加，此时有可能出现 writeIndex 大于 leaderEndIndex 的情况，
                 * 此时单独发送 COMMIT 请求
                 */
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    doCommit();
                    // 检查并追加请求
                    doCheckAppendResponse();
                    break;
                }
                /**
                 * 检测 pendingMap(挂起的请求数量)是否发送泄漏，即挂起队列中容量是否超过允许的最大挂起阀值
                 * 获取当前节点关于本轮次的当前水位线(已成功 append 请求的日志序号)
                 * 如果发现正在挂起请求的日志序号小于水位线，则丢弃。
                 */
                if (pendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                // 如果挂起的请求（等待从节点追加结果）大于 maxPendingSize 时，检查并追加一次 append 请求
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                // 具体的追加请求
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }

        /**
         * 该方法主要就是构建 truncate 请求到从节点
         * @param truncateIndex
         * @throws Exception
         */
        private void doTruncate(long truncateIndex) throws Exception {
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            DLedgerEntry truncateEntry = dLedgerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLedgerResponseCode.UNKNOWN);
            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }

        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                // 如果将目标类型设置为 append，则重置 compareIndex ，并设置 writeIndex 为当前 index 加1
                case APPEND:
                    compareIndex = -1;
                    updatePeerWaterMark(term, peerId, index);
                    quorumAckChecker.wakeup();
                    writeIndex = index + 1;
                    break;
                // 如果将目标类型设置为 COMPARE，则重置 compareIndex 为负一，接下将向各个从节点发送 COMPARE 请求类似，并清除已挂起的请求
                case COMPARE:
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        pendingMap.clear();
                    }
                    break;
                // 如果将目标类型设置为 TRUNCATE，则重置 compareIndex 为负一
                case TRUNCATE:
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        private void doCompare() throws Exception {
            while (true) {
                /**
                 * Step1：验证是否执行，有几个关键点如下：
                 * 1. 判断是否是主节点，如果不是主节点，则直接跳出。
                 * 2. 如果是请求类型不是 COMPARE 或 TRUNCATE 请求，则直接跳出。
                 * 3. 如果已比较索引 和 ledgerEndIndex 都为 -1 ，表示一个新的 DLedger 集群，则直接跳出。
                 */
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {
                    break;
                }
                if (compareIndex == -1 && dLedgerStore.getLedgerEndIndex() == -1) {
                    break;
                }

                /**
                 * Step2：如果 compareIndex 为 -1 或compareIndex 不在有效范围内，
                 * 则重置待比较序列号为当前已已存储的最大日志序号：ledgerEndIndex
                 */
                if (compareIndex == -1) {
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLedgerStore.getLedgerEndIndex() || compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex());
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                }
                /**
                 * Step3：根据序号查询到日志，并向从节点发起 COMPARE 请求，其超时时间为 3s
                 */
                DLedgerEntry entry = dLedgerStore.get(compareIndex);
                PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);

                /**
                 * Step4：根据响应结果计算需要截断的日志序号，其主要实现关键点如下：
                 *
                 */
                long truncateIndex = -1;
                /**
                 * 如果两者的日志序号相同，则无需截断，下次将直接先从节点发送 append 请求
                 * 否则将 truncateIndex 设置为响应结果中的 endIndex
                 */
                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    /*
                     * The comparison is successful:
                     * 1.Just change to append state, if the follower's end index is equal the compared index.
                     * 2.Truncate the follower, if the follower has some dirty entries.
                     */
                    if (compareIndex == response.getEndIndex()) {
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                }
                /**
                 * 如果从节点存储的最大日志序号小于主节点的最小序号，
                 * 或者从节点的最小日志序号大于主节点的最大日志序号，即两者不相交
                 * 这通常发生在从节点崩溃很长一段时间，而主节点删除了过期的条目时
                 * truncateIndex 设置为主节点的 ledgerBeginIndex，即主节点目前最小的偏移量。
                 */
                else if (response.getEndIndex() < dLedgerStore.getLedgerBeginIndex()
                    || response.getBeginIndex() > dLedgerStore.getLedgerEndIndex()) {
                    /*
                     The follower's entries does not intersect with the leader.
                     This usually happened when the follower has crashed for a long time while the leader has deleted the expired entries.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /**
                 * 如果已比较的日志序号小于从节点的开始日志序号，很可能是从节点磁盘发送损耗，从主节点最小日志序号开始同步。
                 */
                else if (compareIndex < response.getBeginIndex()) {
                    /*
                     The compared index is smaller than the follower's begin index.
                     This happened rarely, usually means some disk damage.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /**
                 * 如果已比较的日志序号大于从节点的最大日志序号，则已比较索引设置为从节点最大的日志序号，触发数据的继续同步。
                 */
                else if (compareIndex > response.getEndIndex()) {
                    /*
                     The compared index is bigger than the follower's end index.
                     This happened frequently. For the compared index is usually starting from the end index of the leader.
                     */
                    compareIndex = response.getEndIndex();
                }
                /**
                 * 如果已比较的日志序号大于从节点的开始日志序号，但小于从节点的最大日志序号，则待比较索引减一。
                 */
                else {
                    /*
                      Compare failed and the compared index is in the range of follower's entries.
                     */
                    compareIndex--;
                }
                /**
                 * 如果比较出来的日志序号小于主节点的最小日志需要，则设置为主节点的最小序号
                 */
                if (compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /**
                 * Step5：如果比较出来的日志序号不等于 -1 ，则向从节点发送 TRUNCATE 请求
                 */
                if (truncateIndex != -1) {
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        @Override
        public void doWork() {
            try {
                // 检查状态，是否可以继续发送 append 或 compare
                if (!checkAndFreshState()) {
                    waitForRunning(1);
                    return;
                }
                // 如果推送类型为APPEND，主节点向从节点传播消息请求
                if (type.get() == PushEntryRequest.Type.APPEND) {
                    doAppend();
                }
                // 主节点向从节点发送对比数据差异请求（当一个新节点被选举成为主节点时，往往这是第一步）
                else {
                    // 及时是同步好数据之后再 append
                    // 只是这个操作是在有请求的时候做，不是实例启动的时候就开始做
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                DLedgerUtils.sleep(500);
            }
        }
    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     *
     */
    private class EntryHandler extends ShutdownAbleThread {
        // 上一次检查主服务器是否有 push 消息的时间戳。
        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();
        // 请求处理队列
        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        // COMMIT、COMPARE、TRUNCATE 相关请求
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);

        public EntryHandler(Logger logger) {
            super("EntryHandler", logger);
        }

        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            // The timeout should smaller than the remoting layer's request timeout
            // 首先构建一个响应结果Future，默认超时时间 1s
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                // 如果是 APPEND 请求，放入到 writeRequestMap 集合中
                // 如果已存在该数据结构，说明主节点重复推送，构建返回结果，其状态码为 REPEATED_PUSH。
                // 放入到 writeRequestMap 中，由 doWork 方法定时去处理待写入的请求。
                case APPEND:
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    long index = request.getEntry().getIndex();
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                // 如果是提交请求， 将请求存入 compareOrTruncateRequests 请求处理中，由 doWork 方法异步处理
                case COMMIT:
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                // 如果是 COMPARE 或 TRUNCATE 请求，将待写入队列 writeRequestMap 清空
                // 并将请求放入 compareOrTruncateRequests 请求队列中，由 doWork 方法异步处理。
                case COMPARE:
                case TRUNCATE:
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            return future;
        }

        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getEntry().getIndex());
            }
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        /**
         * 其实现也比较简单，调用DLedgerStore 的 appendAsFollower 方法进行日志的追加
         * 与 appendAsLeader 在日志存储部分相同，只是从节点无需再转发日志
         * @param writeIndex
         * @param request
         * @param future
         */
        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                // 日志追加
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                // 更新 commitIndex
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        /**
         * 处理主节点发送过来的 COMPARE 请求，其实现也比较简单，最终调用 buildResponse 方法构造响应结果
         * 主要也是返回当前从节点的 ledgerBeginIndex、ledgerEndIndex 以及投票轮次，供主节点进行判断比较
         * @param compareIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                DLedgerEntry local = dLedgerStore.get(compareIndex);
                PreConditions.check(request.getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * 处理提交请求，其处理比较简单，就是调用 DLedgerStore 的 updateCommittedIndex 更新其已提交偏移量
         * 故我们还是具体看一下 DLedgerStore 的 updateCommittedIndex 方法。
         * @param committedIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                // 更新从节点的 commitIndex
                dLedgerStore.updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        /**
         * handleDoTruncate 方法实现比较简单，删除从节点上 truncateIndex 日志序号之后的所有日志
         * 具体调用dLedgerStore 的 truncate 方法
         * 简单介绍其实现要点：根据日志序号，去定位到日志文件，如果命中具体的文件，则修改相应的读写指针、刷盘指针等，
         * 并将所在在物理文件之后的所有文件删除。
         * @param truncateIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                long index = dLedgerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         *   * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         *   * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         * leader 将条目推送到follower，并记录推送索引。但在以下情况下，推送可能会停止
         *   * 如果 follower 异常关闭，其 ledger end index 可能比以前小。此时，leader 可以向前推送条目，并一直重试。
         *   * 如果错过了最后一个ack，并且没有新消息传入。leader可以重试推送最后一条消息，但 follower 可以忽略它
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            /**
             * Step1：如果上一次检查的时间距现在不到1s，则跳出；
             * 如果当前没有积压的append请求，同样跳出，
             * 因为可以同样明确的判断出主节点还未推送日志。
             */
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs  = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                return;
            }
            /**
             * Step2：遍历当前待写入的日志追加请求(主服务器推送过来的日志复制请求)，找到需要快速快进的的索引。其关键实现点如下：
             */
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                // 首先获取待写入日志的序号
                long index = pair.getKey().getEntry().getIndex();
                // Fall behind
                // 如果待写入的日志序号小于从节点已追加的日志(endIndex)，并且日志的确已存储在从节点，
                // 则返回成功，并输出警告日志【PushFallBehind】，继续监测下一条待写入日志。
                if (index <= endIndex) {
                    try {
                        DLedgerEntry local = dLedgerStore.get(index);
                        PreConditions.check(pair.getKey().getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(index);
                    continue;
                }
                // Just OK
                // 如果待写入 index 等于 endIndex + 1，则结束循环，因为下一条日志消息已经在待写入队列中，即将写入
                if (index ==  endIndex + 1) {
                    //The next entry is coming, just return
                    return;
                }
                // Fast forward
                // 如果待写入 index 大于 endIndex + 1，并且未超时，则直接检查下一条待写入日志
                TimeoutFuture<PushEntryResponse> future  = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                // 如果待写入 index 大于 endIndex + 1，并且已经超时，则记录该索引，使用 minFastForwardIndex 存储。
                if (index < minFastForwardIndex) {
                    minFastForwardIndex = index;
                }
            }
            /**
             * Step3：如果未找到需要快速失败的日志序号或 writeRequestMap 中未找到其请求，则直接结束检测。
             */
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            /**
             * Step4：则向主节点报告从节点已经与主节点发生了数据不一致
             * 从节点并没有写入序号 minFastForwardIndex 的日志。如果主节点收到此种响应，将会停止日志转发，
             * 转而向各个从节点发送 COMPARE 请求，从而使数据恢复一致。
             */
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        @Override
        public void doWork() {
            try {
                // 如果当前节点的状态不是从节点，则跳出。
                if (!memberState.isFollower()) {
                    waitForRunning(1);
                    return;
                }
                /**
                 * 如果 compareOrTruncateRequests 队列不为空，说明有COMMIT、COMPARE、TRUNCATE 等请求，
                 * 这类请求优先处理。值得注意的是这里使用是 peek、poll 等非阻塞方法，然后根据请求的类型，调用对应的方法。
                 *
                 */
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                }
                /**
                 * 如果只有 append 类请求，则根据当前节点最大的消息序号，尝试从 writeRequestMap 容器中
                 * 获取下一个消息复制请求(ledgerEndIndex + 1) 为 key 去查找
                 * 如果不为空，则执行 doAppend 请求，如果为空
                 * 则调用 checkAbnormalFuture 来处理异常情况
                 */
                else {
                    // TODO 顺序性
                    // follow 是按照顺序从 writeRequestMap 中获取提案的
                    // 所以即使 leader 并发发送了多个不同的提案，follow 也会按照顺序进行执行，保证了顺序性
                    long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
                        // TODO 很重要的方法
                        checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    // 执行 follow 的 append 方法
                    handleDoAppend(nextIndex, request, pair.getValue());
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
