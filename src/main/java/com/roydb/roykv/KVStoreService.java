package com.roydb.roykv;

import com.alibaba.fastjson.JSONObject;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import roykv.KvGrpc;
import roykv.Roykv;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KVStoreService extends KvGrpc.KvImplBase {

    private static final Logger logger = LoggerFactory.getLogger(KVStoreService.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    private RheaKVStore kvStore;

    private RheaKVStore txnStore;

    KVStoreService(RheaKVStore kvStore, RheaKVStore txnStore) {
        this.kvStore = kvStore;
        this.txnStore = txnStore;
    }

    @Override
    public void set(Roykv.SetRequest request, StreamObserver<Roykv.SetReply> responseObserver) {
        //todo different value type

        //todo lock
        long txnId = request.getTxnId();
        if (txnId > 0L) {
            TxnService txnService = new TxnService(kvStore, txnStore);
            byte txnStatus = txnService.getTxnStatus(txnId);
            if (txnStatus == TxnService.TXN_OPEN) {
                JSONObject redoLogObj = new JSONObject();
                redoLogObj.put("opType", "set");
                redoLogObj.put("key", request.getKey());
                redoLogObj.put("value", request.getValue());
                txnService.addTxnRedoLog(txnId, redoLogObj);

                JSONObject undoLogObj = new JSONObject();
                byte[] byteValue = kvStore.bGet(request.getKey(), true);
                if (byteValue == null) {
                    undoLogObj.put("opType", "del");
                } else {
                    undoLogObj.put("opType", "set");
                    undoLogObj.put("value", new String(byteValue));
                }
                undoLogObj.put("key", request.getKey());
                txnService.addTxnUndoLog(txnId, undoLogObj);
            } else {
                throw new RuntimeException(String.format("Txn [%d] status is [%d]", txnId, txnStatus));
            }
        } else {
            boolean result = kvStore.bPut(request.getKey(), request.getValue().getBytes(charset));
            responseObserver.onNext(Roykv.SetReply.newBuilder().setResult(result).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void get(Roykv.GetRequest request, StreamObserver<Roykv.GetReply> responseObserver) {
        byte[] byteValue = kvStore.bGet(request.getKey(), true);

        String value = "";
        if (byteValue != null) {
            value = new String(byteValue);
        }

        responseObserver.onNext(Roykv.GetReply.newBuilder().setValue(value).build());
        responseObserver.onCompleted();
    }

    @Override
    public void exist(Roykv.ExistRequest request, StreamObserver<Roykv.ExistReply> responseObserver) {
        byte[] byteValue = kvStore.bGet(request.getKey(), true);

        boolean existed = byteValue != null;

        responseObserver.onNext(Roykv.ExistReply.newBuilder().setExisted(existed).build());
        responseObserver.onCompleted();
    }

    @Override
    public void scan(Roykv.ScanRequest request, StreamObserver<Roykv.ScanReply> responseObserver) {
        String startKey = "".equals(request.getStartKey()) ? null : request.getStartKey();
        String endKey = "".equals(request.getEndKey()) ? null : request.getEndKey();
        String keyPrefix = request.getKeyPrefix();
        long limit = request.getLimit();

        Roykv.ScanReply.Builder scanReplyBuilder = Roykv.ScanReply.newBuilder();

        int count = 0;

        RheaIterator<KVEntry> iterator = kvStore.iterator(startKey, endKey, (int) limit);
        while (iterator.hasNext()) {
            KVEntry kvEntry = iterator.next();
            String key = new String(kvEntry.getKey());
            if (StringUtils.startsWith(key, keyPrefix)) {
                scanReplyBuilder.addData(Roykv.KVEntry.newBuilder().setKey(key).
                        setValue(new String(kvEntry.getValue())).build());
                ++count;
                if (count >= limit) {
                    break;
                }
            }
        }

        if ((count < limit) && (!("".equals(endKey)))) {
            byte[] lastKeyValue = kvStore.bGet(endKey);
            if (lastKeyValue != null) {
                scanReplyBuilder.addData(Roykv.KVEntry.newBuilder().setKey(endKey).
                        setValue(new String(lastKeyValue)).build());
            }
        }

        responseObserver.onNext(scanReplyBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void mGet(Roykv.MGetRequest request, StreamObserver<Roykv.MGetReply> responseObserver) {
        Roykv.MGetReply.Builder mGetReplyBuilder = Roykv.MGetReply.newBuilder();

        List<byte[]> keys = new ArrayList<byte[]>();

        for (int i = 0; i < request.getKeysCount(); ++i) {
            keys.add(request.getKeys(i).getBytes(charset));
        }

        Map<ByteArray, byte[]> keyValues = kvStore.bMultiGet(keys, true);
        keyValues.forEach((ByteArray k, byte[] v) -> {
            mGetReplyBuilder.putData(new String(k.getBytes()), new String(v));
        });

        responseObserver.onNext(mGetReplyBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAll(Roykv.GetAllRequest request, StreamObserver<Roykv.GetAllReply> responseObserver) {
        Roykv.GetAllReply.Builder getAllReplyBuilder = Roykv.GetAllReply.newBuilder();

        String keyPrefix = request.getKeyPrefix();

        RheaIterator<KVEntry> iterator = kvStore.iterator((String) null, (String) null,10000);
        while (iterator.hasNext()) {
            KVEntry kvEntry = iterator.next();
            String key = new String(kvEntry.getKey());
            if (StringUtils.startsWith(key, keyPrefix)) {
                getAllReplyBuilder.putData(key, new String(kvEntry.getValue()));
            }
        }

        responseObserver.onNext(getAllReplyBuilder.build());
        responseObserver.onCompleted();
    }
}