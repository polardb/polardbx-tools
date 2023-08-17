package com.aliyun.gts.sniffer.common.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HexUtil {
    private static final char[] HEX_CHAR = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    private static String bytesToHexString(byte[] src) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv);
        }
        return sb.toString();
    }

    public static byte[] fromHexString(String hexString) {
        if (null == hexString || "".equals(hexString.trim())) {
            return new byte[0];
        }
        byte[] bytes = new byte[hexString.length() / 2];
        // 16进制字符串
        String hex;
        for (int i = 0; i < hexString.length() / 2; i++) {
            // 每次截取2位
            hex = hexString.substring(i * 2, i * 2 + 2);
            // 16进制-->十进制
            bytes[i] = (byte) Integer.parseInt(hex, 16);
        }
        return bytes;
    }

    public static String bytesToHexString(byte[] src,int start,int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < start+length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv);
        }
        return sb.toString();
    }

    public static String to16MD5(String input){
        return toMD5(input).substring(8,24);
    }
    public static String toMD5(String input) {

        try {
//            拿到一个MD5转换器（如果想要SHA1参数换成”SHA1”）
            MessageDigest messageDigest =MessageDigest.getInstance("MD5");
//            输入的字符串转换成字节数组
            byte[] inputByteArray = input.getBytes();
//            inputByteArray是输入字符串转换得到的字节数组
            messageDigest.update(inputByteArray);
//            转换并返回结果，也是字节数组，包含16个元素
            byte[] resultByteArray = messageDigest.digest();
//            字符数组转换成字符串返回
            return byteArrayToHex(resultByteArray);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    //将字节数组换成成16进制的字符串
    public static String byteArrayToHex(byte[] byteArray) {
        char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9', 'a','b','c','d','e','f' };
        char[] resultCharArray =new char[byteArray.length * 2];
        int index = 0;
        for (byte b : byteArray) {
            resultCharArray[index++] = hexDigits[b>>> 4 & 0xf];
            resultCharArray[index++] = hexDigits[b& 0xf];
        }
        return new String(resultCharArray);
    }


}
