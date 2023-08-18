package com.aliyun.gts.sniffer.common.utils;

import java.util.Arrays;


public class NumberUtil {
    //4字节int转换 Big endian
    public static int fourBEByte2Int(byte[] res,int start){
        int targets = (res[3+start] & 0xff) | (res[2+start] << 8) | (res[1+start] << 16) | (res[0+start] << 24);
        return targets;
    }
//    //4字节int转换 little endian
//    public static int fourLEByte2Int(byte[] res,int start){
//        int targets = (res[start] & 0xff) | (res[start+1] << 8) | (res[2+start] << 16) | (res[3+start] << 24);
//        return targets;
//    }

    public static int fourLEByte2Int(byte[] b,int start){
        long x= ((long) b[start] & 0xff)
                | (((long) b[start+1] & 0xff) << 8)
                | ((long) (b[start+2] & 0xff) << 16)
                | ((long) (b[start+3] & 0xff) << 24);
        return (int)x;
    }

    //2字节int转换
    public static int twoLEByte2Int(byte[] res,int start){
        int targets = (res[start] & 0xff ) + (res[1+start]<<8);
        return targets;
    }
    //3字节int转换
    public static int threeLEByte2Int(byte[] res,int start){
        int targets = (res[start] & 0xff ) | (res[1+start]<<8) | (res[2+start]<<16);
        return targets;
    }

    //3字节int转换
    public static int threeBEByte2Int(byte[] res,int start){
        int targets = (res[2+start] & 0xff ) | (res[1+start]<<8) | (res[0+start]<<16);
        return targets;
    }


    //1字节int转换
    public static int oneByte2Int(byte[] res,int start){
        int targets = (res[start] & 0xff);
        return targets;
    }

    public static long eightLEByteToLong(byte[] input, int offset){
        long value=0;
        // 循环读取每个字节通过移位运算完成long的8个字节拼装
        for(int  count=0;count<8;++count){
            int shift=count<<3;
            value |=((long)0xff<< shift) & ((long)input[offset+count] << shift);
        }
        return value;
    }





    public static int lengthInt(byte pb){
        if((pb & 0xff)==0xfc)
            return 2;
        else if((pb & 0xff)==0xfd)
            return 3;
        else if((pb & 0xff)==0xfe)
            return 8;
        else
            return 1;
    }

    public  byte[] resolveEncodedIntegerByte(byte[] array,int pos){
        int index=pos;
        byte[] result=null;
        if(NumberUtil.lengthInt(array[index])==1){
            result=new byte[1];
            result[0]=array[index];
        } else {
            int length= NumberUtil.lengthInt(array[index++]);
            result= Arrays.copyOfRange(array,index,index+length);
        }
        return result;
    }
    public static long resolveEncodedInteger(byte[] array,int pos){
        int index=pos;
        long result;
        int length= NumberUtil.lengthInt(array[index]);
        switch (length){
            case 1:result= NumberUtil.oneByte2Int(array,index);break;
            case 2:result= NumberUtil.twoLEByte2Int(array,index+1);break;
            case 3:result= NumberUtil.threeLEByte2Int(array,index+1);break;
            case 8:result= NumberUtil.eightLEByteToLong(array,index+1);break;
            default:result=0;
        }
        return result;
    }


    public static float byte2Float(byte[] array,int pos){
        int accum=0;
        accum= accum|(array[pos] & 0xff) << 0;
        accum= accum|(array[pos+1] & 0xff) << 8;
        accum= accum|(array[pos+2] & 0xff) << 16;
        accum= accum|(array[pos+3] & 0xff) << 24;
        return Float.intBitsToFloat(accum);
    }

    public static double byte2Double(byte[] array,int pos){
        long l;
        l = array[pos];
        l &= 0xff;
        l |= ((long) array[pos+1] << 8);
        l &= 0xffff;
        l |= ((long) array[pos+2] << 16);
        l &= 0xffffff;
        l |= ((long) array[pos+3] << 24);
        l &= 0xffffffffl;
        l |= ((long) array[pos+4] << 32);
        l &= 0xffffffffffl;
        l |= ((long) array[pos+5] << 40);
        l &= 0xffffffffffffl;
        l |= ((long) array[pos+6] << 48);
        l &= 0xffffffffffffffl;
        l |= ((long) array[pos+7] << 56);
        return Double.longBitsToDouble(l);
    }
}
