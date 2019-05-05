import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

public class UpLoad {

    @Test
    public void test() throws Exception{
        //step1
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //step2
        fileSystem.copyFromLocalFile(false,new Path("file:///c:\\ShenZao.ini"),new Path("/shenzao/input.ini"));
        //step3
        fileSystem.close();
    }
}
