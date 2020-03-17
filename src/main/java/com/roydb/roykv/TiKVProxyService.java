package com.roydb.roykv;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import roykv.Roykv;
import roykv.TiKVGrpc;
import shade.com.google.protobuf.ByteString;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

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
            if (!(byteValue.isEmpty())) {
                value = byteValue.toStringUtf8();
            }
        }

        responseObserver.onNext(Roykv.GetReply.newBuilder().setValue(value).build());
        responseObserver.onCompleted();
    }

    @Override
    public void exist(Roykv.ExistRequest request, StreamObserver<Roykv.ExistReply> responseObserver) {
        ByteString byteValue = getRawKvClient().get(ByteString.copyFromUtf8(request.getKey()));

        boolean existed = ((byteValue != null) && (!(byteValue.isEmpty())));

        responseObserver.onNext(Roykv.ExistReply.newBuilder().setExisted(existed).build());
        responseObserver.onCompleted();
    }

    @Override
    public void scan(Roykv.ScanRequest request, StreamObserver<Roykv.ScanReply> responseObserver) {
        String startKey = request.getStartKey();
        String startKeyType = request.getStartKeyType();
        String endKey = request.getEndKey();
        String endKeyType = request.getEndKeyType();
        String keyPrefix = request.getKeyPrefix();
        long limit = request.getLimit();

        Roykv.ScanReply.Builder scanReplyBuilder = Roykv.ScanReply.newBuilder();

        int count = 0;

        RawKVClient rawKVClient = getRawKvClient();

        String lastKey = null;

        while (count < limit) {
            List<Kvrpcpb.KvPair> list = null;
            if ("".equals(endKey)) {
                if (lastKey == null) {
                    list = rawKVClient.scan(ByteString.copyFromUtf8(startKey), (int) limit);
                } else {
                    list = rawKVClient.scan(ByteString.copyFromUtf8(lastKey), (int) limit);
                }
            } else {
                if (lastKey == null) {
                    list = rawKVClient.scan(
                            ByteString.copyFromUtf8(startKey),
                            ByteString.copyFromUtf8(endKey)
                    );
                } else {
                    list = rawKVClient.scan(
                            ByteString.copyFromUtf8(lastKey),
                            ByteString.copyFromUtf8(endKey)
                    );
                }
            }

            if (lastKey == null) {
                if (list.size() <= 0) {
                    break;
                }
            } else {
                if (list.size() <= 1) {
                    break;
                }
            }

            for (Kvrpcpb.KvPair kvEntry : list) {
                String key = kvEntry.getKey().toStringUtf8();

                if (lastKey != null) {
                    if (lastKey.equals(key)) {
                        continue;
                    }
                }

                lastKey = key;

                if (!("".equals(endKey))) {
                    if (key.equals(endKey)) {
                        continue;
                    }
                }

                if (StringUtils.startsWith(key, keyPrefix)) {
                    boolean matched = true;
                    String realKey = key.substring(keyPrefix.length());
                    if (!("".equals(startKey))) {
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
                    if (!("".equals(endKey))) {
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
                                setValue(kvEntry.getValue().toStringUtf8()).build());
                        ++count;
                        if (count >= limit) {
                            break;
                        }
                    }
                }
            }
        }

        if ((count < limit) && (!("".equals(endKey)))) {
            ByteString lastKeyValue = rawKVClient.get(ByteString.copyFromUtf8(endKey));
            if ((lastKeyValue != null) && (!(lastKeyValue.isEmpty()))) {
                scanReplyBuilder.addData(Roykv.KVEntry.newBuilder().setKey(endKey).
                        setValue(lastKeyValue.toStringUtf8()).build());
            }
        }

        responseObserver.onNext(scanReplyBuilder.build());
        responseObserver.onCompleted();
    }
}
