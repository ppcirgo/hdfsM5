import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.net.URI;

public class GetFileSys {

    //第1种获取文件系统的方式
    @Test
    public void getFileSystem01() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),configuration);
        System.out.println(fileSystem.toString());
    }

    //第2种获取文件系统的方式
    @Test
    public void getFileSystem02() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(new URI("/"),configuration);
        System.out.println(fileSystem.toString());
    }

    //第3种获取文件系统的方式
    @Test
    public void getFileSystem03() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.newInstance(new URI("file:///"),configuration);
        System.out.println(fileSystem.toString());
    }

    //第4种获取文件系统的方式
    @Test
    public void getFileSystem04() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(new URI("/"),configuration);
        System.out.println(fileSystem.toString());
    }
}
