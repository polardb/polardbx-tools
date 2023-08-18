import com.aliyun.gts.slssniffer.DateUtil;
import org.jetbrains.annotations.TestOnly;
import org.junit.Test;

public class DateTest {

    @Test
    public void test1(){
        String s="1659953787658842";
        long ts=Long.valueOf(s)/1000;
        System.out.println(DateUtil.toChar(ts));


    }


}
