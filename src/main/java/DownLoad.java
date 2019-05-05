import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileOutputStream;
import java.net.URI;

public class DownLoad  {

    //方式一：通过 输入 输出 流
    @Test
    public void test() throws Exception{
        //step1
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //step2
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/input/install.log"));
        FileOutputStream outputStream = new FileOutputStream("c:\\today05.txt");
        int i = IOUtils.copy(fsDataInputStream, outputStream);
        System.out.println(i);
        //step3
        outputStream.close();
        fsDataInputStream.close();
        fileSystem.close();
    }

    //方式二：通过自带的方法 copyToLocalFile
    @Test
    public void test2() throws Exception{
        //step1
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        //step2
        fileSystem.copyToLocalFile(false,new Path("/test/input/install.log"),new Path("file:///c:\\m06999.txt"));
        //step3
        fileSystem.close();
    }
}
