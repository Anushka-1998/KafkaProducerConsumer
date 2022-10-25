package com.kafka.hello.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Class is to Compress the Payload.
 */
@Slf4j
@Component
public class CompressionUtil {


    // this method will be used for compressing data to gzip
    public static byte[] gzipCompress(Object uncompressedData) throws IOException {
        byte[] result = null;
        try {
            // Establish byte array output stream
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            // Establish gzip compressed output stream
            GZIPOutputStream gzout = new GZIPOutputStream(o);
            // Establish object serialization output stream
            ObjectOutputStream out = new ObjectOutputStream(gzout);
            out.writeObject(uncompressedData);
            out.flush();
            out.close();
            gzout.close();
            // return compressed byte stream
            result = o.toByteArray();
            o.close();
        } catch (IOException e) {
            log.error("Error occured while decompressing to gzip", e);
            throw e;
        }
        return result;
    }

    // this method will be used for decompressing gzip data

}
