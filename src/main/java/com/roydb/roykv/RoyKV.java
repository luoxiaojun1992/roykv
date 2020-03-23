package com.roydb.roykv;

import java.io.IOException;

public class RoyKV {
    public static void main(String[] args) throws IOException, InterruptedException {
        String component = args[0];

        if ("kv".equals(component)) {
            String kvConfPath = args[1];
            String grpcPort = args[2];
            KVStore.start(kvConfPath, grpcPort);
        } else if ("pd".equals(component)) {
            String kvConfPath = args[1];
            PDServer.start(kvConfPath);
        }
    }
}
