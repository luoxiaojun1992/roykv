package com.roydb.roykv;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import roykv.Roykv;
import roykv.TxnGrpc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TxnService extends TxnGrpc.TxnImplBase {

    private static final Logger logger = LoggerFactory.getLogger(TxnService.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    private RheaKVStore kvStore;

    TxnService(RheaKVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void begin(Roykv.BeginRequest request, StreamObserver<Roykv.BeginReply> responseObserver) {
        //todo 发号器

        long txnId = System.nanoTime();

        responseObserver.onNext(Roykv.BeginReply.newBuilder().setTxnId(txnId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void commit(Roykv.CommitRequest request, StreamObserver<Roykv.CommitReply> responseObserver) {
        super.commit(request, responseObserver);
    }

    @Override
    public void rollback(Roykv.RollbackRequest request, StreamObserver<Roykv.RollbackReply> responseObserver) {
        super.rollback(request, responseObserver);
    }
}
