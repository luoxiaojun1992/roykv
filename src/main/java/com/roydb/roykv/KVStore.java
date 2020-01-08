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

    public static void main(String[] args) throws IOException {
        //todo refactor conf path from args

        final ObjectMapper mapperNode1 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsNode1 = mapperNode1.readValue(
                new File("src/main/resources/conf/rheakv_node_1_conf"),
                RheaKVStoreOptions.class
        );

        final ObjectMapper mapperNode2 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsNode2 = mapperNode2.readValue(
                new File("src/main/resources/conf/rheakv_node_2_conf"),
                RheaKVStoreOptions.class
        );

        final ObjectMapper mapperNode3 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsNode3 = mapperNode3.readValue(
                new File("src/main/resources/conf/rheakv_node_3_conf"),
                RheaKVStoreOptions.class
        );

        final RheaKVStore rheaKVStoreNode1 = new DefaultRheaKVStore();
        final RheaKVStore rheaKVStoreNode2 = new DefaultRheaKVStore();
        final RheaKVStore rheaKVStoreNode3 = new DefaultRheaKVStore();

        final ObjectMapper mapperTxnNode1 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsTxnNode1 = mapperTxnNode1.readValue(
                new File("src/main/resources/conf/txn_node_1_conf"),
                RheaKVStoreOptions.class
        );

        final ObjectMapper mapperTxnNode2 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsTxnNode2 = mapperTxnNode2.readValue(
                new File("src/main/resources/conf/txn_node_2_conf"),
                RheaKVStoreOptions.class
        );

        final ObjectMapper mapperTxnNode3 = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions optsTxnNode3 = mapperTxnNode3.readValue(
                new File("src/main/resources/conf/txn_node_3_conf"),
                RheaKVStoreOptions.class
        );

        final RheaKVStore rheaTxnStoreNode1 = new DefaultRheaKVStore();
        final RheaKVStore rheaTxnStoreNode2 = new DefaultRheaKVStore();
        final RheaKVStore rheaTxnStoreNode3 = new DefaultRheaKVStore();

        if (rheaKVStoreNode1.init(optsNode1) &&
                rheaKVStoreNode2.init(optsNode2) &&
                rheaKVStoreNode3.init(optsNode3) &&
                rheaTxnStoreNode1.init(optsTxnNode1) &&
                rheaTxnStoreNode2.init(optsTxnNode2) &&
                rheaTxnStoreNode3.init(optsTxnNode3)
        ) {
            //todo rollback failed txn (tick)

            ServerBuilder.forPort(9999)
                    .addService(new KVStoreService(rheaKVStoreNode1, rheaTxnStoreNode1))
                    .addService(new TxnService(rheaKVStoreNode1, rheaTxnStoreNode1))
                    .build()
                    .start();
        }
    }

}
