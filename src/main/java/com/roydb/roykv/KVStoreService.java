package com.roydb.roykv;

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

    KVStoreService(RheaKVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void del(Roykv.DelRequest request, StreamObserver<Roykv.DelReply> responseObserver) {
        long deleted = 0;

        for (String key : request.getKeysList()) {
            if (kvStore.bDelete(key)) {
                ++deleted;
            }
        }

        responseObserver.onNext(Roykv.DelReply.newBuilder().setDeleted(deleted).build());
        responseObserver.onCompleted();
    }

    @Override
    public void set(Roykv.SetRequest request, StreamObserver<Roykv.SetReply> responseObserver) {
        boolean result = kvStore.bPut(request.getKey(), request.getValue().getBytes(charset));
        responseObserver.onNext(Roykv.SetReply.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
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
        String keyPrefix = request.getKeyPrefix();
        String startKey = "".equals(request.getStartKey().substring(keyPrefix.length())) ? null : request.getStartKey();
        String startKeyType = request.getStartKeyType();
        String endKey = "".equals(request.getEndKey()) ? null : request.getEndKey();
        String endKeyType = request.getEndKeyType();

        Roykv.ScanReply.Builder scanReplyBuilder = Roykv.ScanReply.newBuilder();

        if ((startKey != null) && (startKey.equals(endKey))) {
            byte[] byteValue = kvStore.bGet(startKey, true);
            if (byteValue != null) {
                scanReplyBuilder.addData(Roykv.KVEntry.newBuilder().setKey(startKey).setValue(new String(byteValue)).build());
            }

            responseObserver.onNext(scanReplyBuilder.build());
            responseObserver.onCompleted();
        }

        String scanStartString = keyPrefix;
        String scanEndString = null;
        if ("string".equals(startKeyType)) {
            scanStartString = startKey;
        }
        if ("string".equals(endKeyType)) {
            scanEndString = endKey;
        }

        long limit = request.getLimit();

        long count = 0;

        //此处limit是buffer size,并不限制扫描行数
        RheaIterator<KVEntry> iterator = kvStore.iterator(
                scanStartString, scanEndString, (int)(limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : limit)
        );
        while (iterator.hasNext()) {
            KVEntry kvEntry = iterator.next();
            String key = new String(kvEntry.getKey());
            if (StringUtils.startsWith(key, keyPrefix)) {
                boolean matched = true;
                String realKey = key.substring(keyPrefix.length());
                if (startKey != null) {
                    String realStartKey = startKey.substring(keyPrefix.length());
                    if ("integer".equals(startKeyType)) {
                        if (Integer.parseInt(realKey) < Integer.parseInt(realStartKey)) {
                            matched = false;
                        }
                    } else if ("double".equals(startKeyType)) {
                        if (Double.parseDouble(realKey) < Double.parseDouble(realStartKey)) {
                            matched = false;
                        }
                    }
                }
                if (endKey != null) {
                    String realEndKey = endKey.substring(keyPrefix.length());
                    if ("integer".equals(endKeyType)) {
                        if (Integer.parseInt(realKey) > Integer.parseInt(realEndKey)) {
                            matched = false;
                        }
                    } else if ("double".equals(endKeyType)) {
                        if (Double.parseDouble(realKey) > Double.parseDouble(realEndKey)) {
                            matched = false;
                        }
                    }
                }

                if (matched) {
                    scanReplyBuilder.addData(Roykv.KVEntry.newBuilder().setKey(key).
                            setValue(new String(kvEntry.getValue())).build());
                    ++count;
                    if (count >= limit) {
                        break;
                    }
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

        RheaIterator<KVEntry> iterator = kvStore.iterator((String) null, (String) null, 10000);
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
        //todo handle key type
        String keyPrefix = request.getKeyPrefix();
        String startKey = "".equals(request.getStartKey().substring(keyPrefix.length())) ? null : request.getStartKey();
        String endKey = "".equals(request.getEndKey()) ? null : request.getEndKey();

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
        long timeout = 10;

        DistributedLock<byte[]> lock = kvStore.getDistributedLock("lock:key:" + request.getKey(), timeout, TimeUnit.SECONDS);

        if (lock.tryLock(timeout, TimeUnit.SECONDS)) {
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
}
