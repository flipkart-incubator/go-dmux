package com.flipkart.godmux.tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class BatchDecoderTest {

    // Uses file generated by goTestcase
    public void testBatchDecoding() throws IOException {
        byte[] data = new byte[1024 * 1024];
        int counter = 0;
        try (FileInputStream fs = new FileInputStream(new File("/tmp/dat1"))) {
            try (BufferedInputStream bos = new BufferedInputStream(fs)) {
                int val = -1;
                do {
                    val = bos.read(data);
                    if (val != -1)
                        counter += val;
                } while (val != -1);
            }
        }

        byte[] payload = Arrays.copyOfRange(data, 0, counter);
        byte[][] output = BatchDecoder.DECODE.decode(payload);
        for (int i = 0; i < output.length; i++) {
            System.out.Println(new String(output[i]));
        }
    }
}
