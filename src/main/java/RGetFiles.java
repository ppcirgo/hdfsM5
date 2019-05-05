import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.net.URI;

//递归获取hdfs的文件（夹）
public class RGetFiles {
    @Test
    public void test() throws Exception{
        //step1
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"),configuration);
        //step2
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()){
            LocatedFileStatus fileStatus = files.next();
            System.out.println("Name :"+fileStatus.getPath());
            System.out.println("Owner:"+fileStatus.getOwner());
            System.out.println("Group:"+fileStatus.getGroup());
            System.out.println("copys:"+fileStatus.getReplication());
        }
        //step3
        fs.close();
    }
}
