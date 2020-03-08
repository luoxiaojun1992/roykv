package com.roydb.roykv;

import java.io.IOException;

public class RoyKV {
    public static void main(String[] args) throws IOException {
        String component = args[0];
        String kvConfPath = args[1];

        if ("kv".equals(component)) {
            String grpcPort = args[2];
            KVStore.start(kvConfPath, grpcPort);
        } else if ("pd".equals(component)) {
            PDServer.start(kvConfPath);
        }
    }
}
