package com.roydb.roykv;

import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import roykv.KvGrpc;
import roykv.Roykv;

import java.nio.charset.Charset;

public class KVStoreService extends KvGrpc.KvImplBase {

    private static final Logger logger = LoggerFactory.getLogger(KVStore.class);
    private static final Charset charset = Charset.forName("utf-8");

    private RheaKVStore kvStore;

    KVStoreService(RheaKVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void get(Roykv.GetRequest request, StreamObserver<Roykv.GetReply> responseObserver) {
        byte[] bytesValNode1 = kvStore.bGet(request.getKey(), true);

        String value = "";
        if (bytesValNode1 != null) {
            value = new String(bytesValNode1);
        }

        responseObserver.onNext(Roykv.GetReply.newBuilder().setValue(value).build());
        responseObserver.onCompleted();
    }

    @Override
    public void exist(Roykv.ExistRequest request, StreamObserver<Roykv.ExistReply> responseObserver) {
        byte[] bytesValNode1 = kvStore.bGet(request.getKey(), true);

        boolean existed = bytesValNode1 != null;

        responseObserver.onNext(Roykv.ExistReply.newBuilder().setExisted(existed).build());
        responseObserver.onCompleted();
    }

    @Override
    public void scan(Roykv.ScanRequest request, StreamObserver<Roykv.ScanReply> responseObserver) {
        String startKey = "".equals(request.getStartKey()) ? null : request.getStartKey();
        String endKey = "".equals(request.getEndKey()) ? null : request.getEndKey();
        long limit = request.getLimit();

        Roykv.ScanReply.Builder scanReplyBuilder = Roykv.ScanReply.newBuilder();

        RheaIterator iterator = kvStore.iterator(startKey, endKey, (int) limit);
        while (iterator.hasNext()) {
            KVEntry kvEntry = (KVEntry) iterator.next();
            scanReplyBuilder.putData(new String(kvEntry.getKey()), new String(kvEntry.getValue()));
        }

        if ((!("".equals(endKey))) && (scanReplyBuilder.getDataMap().size() < limit)) {
            byte[] lastKeyValue = kvStore.bGet(endKey);
            if (lastKeyValue != null) {
                scanReplyBuilder.putData(endKey, new String(lastKeyValue));
            }
        }

        responseObserver.onNext(scanReplyBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void mGet(Roykv.MGetRequest request, StreamObserver<Roykv.MGetReply> responseObserver) {
        super.mGet(request, responseObserver);
    }

    @Override
    public void getAll(Roykv.GetAllRequest request, StreamObserver<Roykv.GetAllReply> responseObserver) {
        super.getAll(request, responseObserver);
    }
}