package com.roydb.roykv;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import roykv.Roykv;
import roykv.TxnGrpc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TxnService extends TxnGrpc.TxnImplBase {

    public static final byte TXN_OPEN = 0;
    public static final byte TXN_COMMITTED = 1;
    public static final byte TXN_CANCELED = 2;


    private static final Logger logger = LoggerFactory.getLogger(TxnService.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    private RheaKVStore kvStore;

    private RheaKVStore txnStore;

    TxnService(RheaKVStore kvStore, RheaKVStore txnStore) {
        this.kvStore = kvStore;
        this.txnStore = txnStore;
    }

    @Override
    public void begin(Roykv.BeginRequest request, StreamObserver<Roykv.BeginReply> responseObserver) {
        //todo 发号器

        long txnId = System.nanoTime();

        responseObserver.onNext(Roykv.BeginReply.newBuilder().setTxnId(txnId).build());
        responseObserver.onCompleted();
    }

    private byte[] getTxn(long txnId) {
        return txnStore.bGet("txn::" + String.valueOf(txnId), true);
    }

    public boolean setTxn(long txnId, JSONObject txn) {
        return txnStore.bPut("txn::" + String.valueOf(txnId), JSON.toJSONBytes(txn));
    }

    private boolean executeOpLog(String log) {
        //todo batch put 考虑失败后单个op重试

        boolean result = true;

        JSONArray logArr = JSON.parseArray(log);

        int logSize = logArr.size();
        for (int logIndex = 0; logIndex < logSize; ++logIndex) {
            JSONObject logObj = logArr.getJSONObject(logIndex);
            String opType = logObj.getString("opType");
            String key = logObj.getString("key");
            switch (opType) {
                case "set":
                    if ((kvStore.bGet(key) != null) &&
                            (!kvStore.bPut(key, logObj.getBytes("value")))
                    ) {
                        result = false;
                    }
                    break;
                case "del":
                    if ((kvStore.bGet(key) != null) &&
                            (!kvStore.bDelete(key))
                    ) {
                        result = false;
                    }
                    break;
                default:
            }

            if (!result) {
                break;
            }
        }

        return result;
    }

    private boolean executeRedoLog(long txnId) {
        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        String txnRedoLog = txnObj.getString("redoLog");

        return executeOpLog(txnRedoLog);
    }

    private boolean executeUndoLog(long txnId) {
        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        String txnUndoLog = txnObj.getString("undoLog");

        return executeOpLog(txnUndoLog);
    }

    public boolean addTxnRedoLog(long txnId, JSONObject redoLog) {
        //todo lock

        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        txnObj.put("redoLog", redoLog);

        return setTxn(txnId, txnObj);
    }

    public boolean addTxnUndoLog(long txnId, JSONObject undoLog) {
        //todo lock

        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        txnObj.put("undoLog", undoLog);

        return setTxn(txnId, txnObj);
    }

    private boolean commitTxn(long txnId) {
        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        txnObj.put("status", TXN_COMMITTED);

        return setTxn(txnId, txnObj);
    }

    public boolean rollbackTxn(long txnId) {
        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        txnObj.put("status", TXN_CANCELED);

        return setTxn(txnId, txnObj);
    }

    public byte getTxnStatus(long txnId) {
        byte[] byteTxn = getTxn(txnId);
        if (byteTxn == null) {
            throw new RuntimeException(String.format("Txn[%d] not exists.", txnId));
        }

        JSONObject txnObj = JSON.parseObject(new String(byteTxn));
        return txnObj.getByte("status");
    }

    @Override
    public void commit(Roykv.CommitRequest request, StreamObserver<Roykv.CommitReply> responseObserver) {
        //todo lock

        long txnId = request.getTxnId();

        boolean result = executeRedoLog(txnId);

        if (!result) {
            executeUndoLog(txnId);
            rollbackTxn(txnId);
        } else {
            result = commitTxn(txnId);
        }

        responseObserver.onNext(Roykv.CommitReply.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void rollback(Roykv.RollbackRequest request, StreamObserver<Roykv.RollbackReply> responseObserver) {
        //todo lock

        long txnId = request.getTxnId();

        boolean result = executeUndoLog(txnId);

        if (result) {
            result = rollbackTxn(txnId);
        }

        responseObserver.onNext(Roykv.RollbackReply.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }
}
