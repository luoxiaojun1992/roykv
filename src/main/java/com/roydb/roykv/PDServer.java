package com.roydb.roykv;

import com.alipay.sofa.jraft.rhea.PlacementDriverServer;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class PDServer {

    private static final Logger logger = LoggerFactory.getLogger(PDServer.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    public static void start(String kvConfPath) throws IOException {
        final ObjectMapper mapperNode1 = new ObjectMapper(new YAMLFactory());
        final PlacementDriverServerOptions optsNode1 = mapperNode1.readValue(
                new File(kvConfPath),
                PlacementDriverServerOptions.class
        );

        final PlacementDriverServer PDNode1 = new PlacementDriverServer();

        PDNode1.init(optsNode1);
    }

}
