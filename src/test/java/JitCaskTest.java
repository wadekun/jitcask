import org.junit.Assert;
import org.junit.Test;
import xyz.liangck.jitcask.JitCask;

import java.io.IOException;

/**
 * @author: liangck
 * @description:
 */
public class JitCaskTest {
    private JitCask open(String dir) throws IOException {
        JitCask jitCask = JitCask.open(dir);
        jitCask.merge();
        return jitCask;
    }

    @Test
    public void simpleTest() throws IOException {
        JitCask jitCask = open("data");
        String value = "hello bitcask";
        Boolean put = jitCask.put("key", value);
        Assert.assertEquals(jitCask.get("key"), value);
    }
}
