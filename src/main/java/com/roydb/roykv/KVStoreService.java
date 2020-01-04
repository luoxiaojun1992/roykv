package com.roydb.roykv;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
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
        byte[] bytesValNode1 = kvStore.bGet(request.getKey().getBytes(charset));

        String value = "";
        if (bytesValNode1 != null) {
            value = new String(bytesValNode1);
        }

        responseObserver.onNext(Roykv.GetReply.newBuilder().setValue(value).build());
        responseObserver.onCompleted();
    }
}
