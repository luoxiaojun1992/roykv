package com.roydb.roykv;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KVStore {

    private static final Logger logger = LoggerFactory.getLogger(KVStore.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    public static void start(String confPath) throws IOException {
        final ObjectMapper mapperNode1 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsNode1 = mapperNode1.readValue(
                new File(confPath),
                RheaKVStoreOptions.class
        );

        final RheaKVStore rheaKVStoreNode1 = new DefaultRheaKVStore();

//        final ObjectMapper mapperTxnNode1 = new ObjectMapper(new YAMLFactory());
//        final RheaKVStoreOptions optsTxnNode1 = mapperTxnNode1.readValue(
//                new File("src/main/resources/conf/txn_node_1_conf"),
//                RheaKVStoreOptions.class
//        );
//
//        final ObjectMapper mapperTxnNode2 = new ObjectMapper(new YAMLFactory());
//        final RheaKVStoreOptions optsTxnNode2 = mapperTxnNode2.readValue(
//                new File("src/main/resources/conf/txn_node_2_conf"),
//                RheaKVStoreOptions.class
//        );
//
//        final ObjectMapper mapperTxnNode3 = new ObjectMapper(new YAMLFactory());
//        final RheaKVStoreOptions optsTxnNode3 = mapperTxnNode3.readValue(
//                new File("src/main/resources/conf/txn_node_3_conf"),
//                RheaKVStoreOptions.class
//        );
//
//        final RheaKVStore rheaTxnStoreNode1 = new DefaultRheaKVStore();
//        final RheaKVStore rheaTxnStoreNode2 = new DefaultRheaKVStore();
//        final RheaKVStore rheaTxnStoreNode3 = new DefaultRheaKVStore();

        if (rheaKVStoreNode1.init(optsNode1)) {
            //todo rollback failed txn (tick)

            ServerBuilder.forPort(50053)
                    .addService(new KVStoreService(rheaKVStoreNode1))
                    .build()
                    .start();
        }
    }

}
