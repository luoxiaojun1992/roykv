package com.roydb.roykv;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import roykv.Roykv;
import roykv.TiKVGrpc;
import shade.com.google.protobuf.ByteString;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TiKVProxyService extends TiKVGrpc.TiKVImplBase {

    private static final Logger logger = LoggerFactory.getLogger(TiKVProxyService.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    private String pdAddress;

    TiKVProxyService(String pdAddress) {
        this.pdAddress = pdAddress;
    }

    private RawKVClient getRawKvClient() {
        TiConfiguration conf = TiConfiguration.createRawDefault(pdAddress);
        TiSession session = TiSession.create(conf);
        return session.createRawClient();
    }

    @Override
    public void del(Roykv.DelRequest request, StreamObserver<Roykv.DelReply> responseObserver) {
        long deleted = 0;

        RawKVClient rawKVClient = getRawKvClient();

        for (String key : request.getKeysList()) {
            rawKVClient.delete(ByteString.copyFromUtf8(key));
            ++deleted;
        }

        responseObserver.onNext(Roykv.DelReply.newBuilder().setDeleted(deleted).build());
        responseObserver.onCompleted();
    }

    @Override
    public void set(Roykv.SetRequest request, StreamObserver<Roykv.SetReply> responseObserver) {
        getRawKvClient().put(
                ByteString.copyFromUtf8(request.getKey()),
                ByteString.copyFromUtf8(request.getValue())
        );
        responseObserver.onNext(Roykv.SetReply.newBuilder().setResult(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(Roykv.GetRequest request, StreamObserver<Roykv.GetReply> responseObserver) {
        ByteString byteValue = getRawKvClient().get(ByteString.copyFromUtf8(request.getKey()));

        String value = "";
        if (byteValue != null) {
            if (!byteValue.isEmpty()) {
                value = byteValue.toStringUtf8();
            }
        }

        responseObserver.onNext(Roykv.GetReply.newBuilder().setValue(value).build());
        responseObserver.onCompleted();
    }

    @Override
    public void exist(Roykv.ExistRequest request, StreamObserver<Roykv.ExistReply> responseObserver) {
        ByteString byteValue = getRawKvClient().get(ByteString.copyFromUtf8(request.getKey()));

        boolean existed = ((byteValue != null) && (!byteValue.isEmpty()));

        responseObserver.onNext(Roykv.ExistReply.newBuilder().setExisted(existed).build());
        responseObserver.onCompleted();
    }


}
