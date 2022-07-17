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

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerProtocolHander;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dledger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.PreConditions;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerServer implements DLedgerProtocolHander {

    private static Logger logger = LoggerFactory.getLogger(DLedgerServer.class);

    private MemberState memberState;
    private DLedgerConfig dLedgerConfig;

    private DLedgerStore dLedgerStore;
    private DLedgerRpcService dLedgerRpcService;
    private DLedgerEntryPusher dLedgerEntryPusher;
    private DLedgerLeaderElector dLedgerLeaderElector;

    public DLedgerServer(DLedgerConfig dLedgerConfig) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = new MemberState(dLedgerConfig);
        // 存储服务
        this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);
        // RPC 服务
        dLedgerRpcService = new DLedgerRpcNettyService(this);
        // 日志复制
        dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
        // leader选举
        dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);
    }



    public void startup() {
        // 存储服务启动
        this.dLedgerStore.startup();
        // RPC 服务启动
        this.dLedgerRpcService.startup();
        // 日志复制服务启动
        this.dLedgerEntryPusher.startup();
        // leader选举启动
        this.dLedgerLeaderElector.startup();
    }

    public void shutdown() {
        this.dLedgerLeaderElector.shutdown();
        this.dLedgerEntryPusher.shutdown();
        this.dLedgerRpcService.shutdown();
        this.dLedgerStore.shutdown();
    }

    private DLedgerStore createDLedgerStore(String storeType, DLedgerConfig config, MemberState memberState) {
        if (storeType.equals(DLedgerConfig.MEMORY)) {
            return new DLedgerMemoryStore(config, memberState);
        } else {
            return new DLedgerMmapFileStore(config, memberState);
        }
    }

    public MemberState getMemberState() {
        return memberState;
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        try {

            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleHeartBeat(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleHeartBeat] failed", memberState.getSelfId(), e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleVote(request, false);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleVote] failed", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * Handle the append requests:
     *  1.append the entry to local store Leader 节点存储消息
     *  2.submit the future to entry pusher and wait the quorum ack 主节点等待从节点复制 ACK
     *  3.if the pending requests are full, then reject it immediately 判断 Push 队列是否已满
     * @param request
     * @return
     * @throws IOException
     */
    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws IOException {
        try {
            // Step1:首先验证请求的合理性
            // 如果请求的节点 ID 不是当前处理节点，则抛出异常
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            // 如果请求的集群不是当前节点所在的集群，则抛出异常
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            // 如果当前节点不是主节点，则抛出异常
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            long currTerm = memberState.currTerm();
            // 如果预处理队列已经满了，则拒绝客户端请求，返回 LEADER_PENDING_FULL 错误码
            if (dLedgerEntryPusher.isPendingFull(currTerm)) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setGroup(memberState.getGroup());
                appendEntryResponse.setCode(DLedgerResponseCode.LEADER_PENDING_FULL.getCode());
                appendEntryResponse.setTerm(currTerm);
                appendEntryResponse.setLeaderId(memberState.getSelfId());
                return AppendFuture.newCompletedFuture(-1, appendEntryResponse);
            }
            // 如果未满，将请求封装成 DLedgerEntry，则调用 dLedgerStore 方法追加日志
            // 并且通过使用 dLedgerEntryPusher 的 waitAck 方法同步等待副本节点的复制响应，并最终将结果返回给调用方法
            else {
                DLedgerEntry dLedgerEntry = new DLedgerEntry();
                dLedgerEntry.setBody(request.getBody());
                DLedgerEntry resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
                return dLedgerEntryPusher.waitAck(resEntry);
            }
        } catch (DLedgerException e) {
            logger.error("[{}][HandleAppend] failed", memberState.getSelfId(), e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            DLedgerEntry entry = dLedgerStore.get(request.getBeginIndex());
            GetEntriesResponse response = new GetEntriesResponse();
            response.setGroup(memberState.getGroup());
            if (entry != null) {
                response.setEntries(Collections.singletonList(entry));
            }
            return CompletableFuture.completedFuture(response);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleGet] failed", memberState.getSelfId(), e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setLeaderId(memberState.getLeaderId());
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            MetadataResponse metadataResponse = new MetadataResponse();
            metadataResponse.setGroup(memberState.getGroup());
            metadataResponse.setPeers(memberState.getPeerMap());
            metadataResponse.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(metadataResponse);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleMetadata] failed", memberState.getSelfId(), e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) {
        return null;
    }

    @Override public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerEntryPusher.handlePush(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandlePush] failed", memberState.getSelfId(), e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    public DLedgerStore getdLedgerStore() {
        return dLedgerStore;
    }

    public DLedgerRpcService getdLedgerRpcService() {
        return dLedgerRpcService;
    }

    public DLedgerLeaderElector getdLedgerLeaderElector() {
        return dLedgerLeaderElector;
    }

    public DLedgerConfig getdLedgerConfig() {
        return dLedgerConfig;
    }
}
