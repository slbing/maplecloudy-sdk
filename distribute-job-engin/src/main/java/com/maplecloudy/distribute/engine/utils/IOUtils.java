package com.maplecloudy.distribute.engine.utils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public abstract class IOUtils {

    public static String readFrom(InputStream stream) {
        try {
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, Charset.forName("UTF-8")));
            String line;

            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            return sb.toString();
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot read data stream", ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                // ignore
            }
        }
    }


    public static void close(Closeable closable) {
        if (closable != null) {
            try {
                closable.close();
            } catch (IOException e) {
                // silently ignore
            }
        }
    }
}
