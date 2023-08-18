package com.aliyun.gts;

import com.aliyun.gts.sniffer.common.utils.NumberUtil;
import org.testng.annotations.Test;

public class BinaryValueTest {
    private boolean isBitmapNull(byte[] nullBitmap,int args){
        int flag=args/8;
        int nullBitmapValue= NumberUtil.oneByte2Int(nullBitmap,flag);
        int oneByteOffset=args%8;
        int compareByte=1<<oneByteOffset;
        if((nullBitmapValue & compareByte)==compareByte){
            return true;
        }
        return false;
    }
    @Test
    public void test(){
        byte[] nullBitmap=new byte[2];
        nullBitmap[0]=-86;
        nullBitmap[1]=2;

        isBitmapNull(nullBitmap,0);

    }






}
