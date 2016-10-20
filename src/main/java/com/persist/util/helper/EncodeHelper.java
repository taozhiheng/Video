package com.persist.util.helper;

import java.security.MessageDigest;

/**
 * Created by taozhiheng on 16-10-13.
 *
 */
public class EncodeHelper {

    /**
     * md5 encode
     * @deprecated This method is not safe enough, use {@link #encode(String)} instead.
     * */
    public static String MD5(String input) {
        if(input == null)
            return null;
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }
        char[] charArray = input.toCharArray();
        byte[] byteArray = new byte[charArray.length];

        for (int i = 0; i < charArray.length; i++)
            byteArray[i] = (byte) charArray[i];

        byte[] md5Bytes = md5.digest(byteArray);

        StringBuilder hexValue = new StringBuilder();

        for (int i = 0; i < md5Bytes.length; i++)
        {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16)
                hexValue.append("0");
            hexValue.append(Integer.toHexString(val));
        }

        return hexValue.toString();
    }

    /**
     * symmetry encode and decode.
     * For example:
     * String src = "abc";
     * String secret = symmetry(src);
     * String reality = symmetry(secret);// reality.equal(src) == true
     * */
    public static String symmetry(String input)
    {
        if(input == null)
            return null;
        char[] a = input.toCharArray();
        for (int i = 0; i < a.length; i++) {
            a[i] = (char) (a[i] ^ 't');
        }
        return new String(a);
    }

    /**
     * use both MD5 and symmetry
     * */
    public static String encode(String input)
    {
        return symmetry(MD5(input));
    }
}
