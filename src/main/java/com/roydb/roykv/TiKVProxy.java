package com.roydb.roykv;

import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TiKVProxy {
    private static final Logger logger = LoggerFactory.getLogger(TiKVProxy.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    public static void start(String pdAddress, String grpcPort) throws IOException {
        ServerBuilder.forPort(Integer.parseInt(grpcPort))
                .addService(new TiKVProxyService(pdAddress))
                .build()
                .start();
    }
}
