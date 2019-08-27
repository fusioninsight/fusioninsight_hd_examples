package com.huawei.hadoop.webhdfs.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.BitSet;

public class URLUtil {
    /**
     * Array containing the safe characters set as defined by RFC 1738
     */
    private static BitSet safeCharacters;

    private static final char[] hexadecimal = {'0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    static {
        safeCharacters = new BitSet(256);
        int i;
        // 'lowalpha' rule
        for (i = 'a'; i <= 'z'; i++) {
            safeCharacters.set(i);
        }
        // 'hialpha' rule
        for (i = 'A'; i <= 'Z'; i++) {
            safeCharacters.set(i);
        }
        // 'digit' rule
        for (i = '0'; i <= '9'; i++) {
            safeCharacters.set(i);
        }

        // 'safe' rule
        safeCharacters.set('$');
        safeCharacters.set('-');
        safeCharacters.set('_');
        safeCharacters.set('.');
        safeCharacters.set('+');

        // 'extra' rule
        safeCharacters.set('!');
        safeCharacters.set('*');
        safeCharacters.set('\'');
        safeCharacters.set('(');
        safeCharacters.set(')');
        safeCharacters.set(',');

        // special characters common to http: file: and ftp: URLs ('fsegment'
        // and 'hsegment' rules)
        safeCharacters.set('/');
        safeCharacters.set(':');
        safeCharacters.set('@');
        safeCharacters.set('&');
        safeCharacters.set('=');
    }

    /**
     * Encode a path as required by the URL specification (<a
     * href="http://www.ietf.org/rfc/rfc1738.txt"> RFC 1738</a>). This differs
     * from <code>java.net.URLEncoder.encode()</code> which encodes according to
     * the <code>x-www-form-urlencoded</code> MIME format.
     *
     * @param path the path to encode
     * @return the encoded path
     */
    public static String encodePath(String path) {
        // stolen from org.apache.catalina.servlets.DefaultServlet ;)

        /**
         * Note: Here, ' ' should be encoded as "%20" and '/' shouldn't be
         * encoded.
         */

        int maxBytesPerChar = 10;
        StringBuffer rewrittenPath = new StringBuffer(path.length());
        ByteArrayOutputStream buf = new ByteArrayOutputStream(maxBytesPerChar);
        OutputStreamWriter writer;
        try {
            writer = new OutputStreamWriter(buf, "UTF8");
        } catch (Exception e) {
            e.printStackTrace();
            writer = new OutputStreamWriter(buf);
        }

        for (int i = 0; i < path.length(); i++) {
            int c = path.charAt(i);
            if (safeCharacters.get(c)) {
                rewrittenPath.append((char) c);
            } else {
                // convert to external encoding before hex conversion
                try {
                    writer.write(c);
                    writer.flush();
                } catch (IOException e) {
                    buf.reset();
                    continue;
                }
                byte[] ba = buf.toByteArray();
                for (int j = 0; j < ba.length; j++) {
                    // Converting each byte in the buffer
                    byte toEncode = ba[j];
                    rewrittenPath.append('%');
                    int low = (toEncode & 0x0f);
                    int high = ((toEncode & 0xf0) >> 4);
                    rewrittenPath.append(hexadecimal[high]);
                    rewrittenPath.append(hexadecimal[low]);
                }
                buf.reset();
            }
        }
        return rewrittenPath.toString();
    }

    public static void main(String[] args) {
        System.out.println(URLUtil.encodePath("zen/我的图片"));
    }
}