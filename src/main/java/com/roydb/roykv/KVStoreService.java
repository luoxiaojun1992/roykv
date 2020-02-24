package com.roydb.roykv;

import com.alibaba.fastjson.JSONObject;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
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
import java.util.concurrent.TimeUnit;

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
                if (!txnService.addTxnRedoLog(txnId, redoLogObj)) {
                    responseObserver.onNext(Roykv.SetReply.newBuilder().setResult(false).build());
                } else {
                    JSONObject undoLogObj = new JSONObject();
                    byte[] byteValue = kvStore.bGet(request.getKey(), true);
                    if (byteValue == null) {
                        undoLogObj.put("opType", "del");
                    } else {
                        undoLogObj.put("opType", "set");
                        undoLogObj.put("value", new String(byteValue));
                    }
                    undoLogObj.put("key", request.getKey());
                    responseObserver.onNext(
                            Roykv.SetReply.newBuilder().setResult(txnService.addTxnUndoLog(txnId, undoLogObj)).build()
                    );
                }
                responseObserver.onCompleted();
            } else {
                throw new RuntimeException(String.format("Txn[%d] status is [%d]", txnId, txnStatus));
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

        if ((count < limit) && (endKey != null)) {
            byte[] lastKeyValue = kvStore.bGet(endKey, true);
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

    @Override
    public void count(Roykv.CountRequest request, StreamObserver<Roykv.CountReply> responseObserver) {
        String startKey = "".equals(request.getStartKey()) ? null : request.getStartKey();
        String endKey = "".equals(request.getEndKey()) ? null : request.getEndKey();
        String keyPrefix = request.getKeyPrefix();

        Roykv.CountReply.Builder countReplyBuilder = Roykv.CountReply.newBuilder();

        long count = 0;

        RheaIterator<KVEntry> iterator = kvStore.iterator(startKey, endKey, 10000);
        while (iterator.hasNext()) {
            KVEntry kvEntry = iterator.next();
            String key = new String(kvEntry.getKey());
            if (StringUtils.startsWith(key, keyPrefix)) {
                ++count;
            }
        }

        if (endKey != null) {
            byte[] lastKeyValue = kvStore.bGet(endKey, true);
            if (lastKeyValue != null) {
                ++count;
            }
        }

        responseObserver.onNext(countReplyBuilder.setCount(count).build());
        responseObserver.onCompleted();
    }

    @Override
    public void incr(Roykv.IncrRequest request, StreamObserver<Roykv.IncrReply> responseObserver) {
        DistributedLock<byte[]> lock = kvStore.getDistributedLock("lock:key:" + request.getKey(), 10, TimeUnit.SECONDS);

        if (lock.tryLock()) {
            try {
                byte[] value = kvStore.bGet(request.getKey(), true);
                String strVal = new String(value);
                long intVal = Long.parseLong(strVal);
                intVal = intVal + request.getStep();

                if (kvStore.bPut(request.getKey(), String.valueOf(intVal).getBytes(charset))) {
                    responseObserver.onNext(Roykv.IncrReply.newBuilder().setResult(intVal).build());
                } else {
                    responseObserver.onNext(Roykv.IncrReply.newBuilder().setResult(0).build());
                }
                responseObserver.onCompleted();
            } finally {
                lock.unlock();
            }
        } else {
            responseObserver.onNext(Roykv.IncrReply.newBuilder().setResult(0).build());
            responseObserver.onCompleted();
        }
    }

    //todo distributed lock & unlock using rheakv
}
