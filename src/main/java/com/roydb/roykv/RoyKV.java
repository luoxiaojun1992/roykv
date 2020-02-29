package com.roydb.roykv;

import java.io.IOException;

public class RoyKV {
    public static void main(String[] args) throws IOException {
        String component = args[0];
        String confPath = args[1];

        if ("kv".equals(component)) {
            KVStore.start(confPath);
        } else if ("pd".equals(component)) {
            PDServer.start(confPath);
        }
    }
}
