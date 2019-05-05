import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.URI;

public class MergeFile {
    //将多个本地系统文件，上传到hdfs，并合并成一个大的文件
    public static void main(String[] args) throws Exception{
        //step1
        FileSystem hdfsFileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());//本地文件系统
        //step2
        FSDataOutputStream fsDataOutputStream = hdfsFileSystem.create(new Path("/bigFile.xml"));
        RemoteIterator<LocatedFileStatus> listFiles = localFileSystem.listFiles(new Path("Y:\\Bigdata_Software\\test\\uploadLittleFile"),false);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            FSDataInputStream fsDataInputStream = localFileSystem.open(fileStatus.getPath());
            IOUtils.copy(fsDataInputStream,fsDataOutputStream);
            IOUtils.closeQuietly(fsDataInputStream);
        }
        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        hdfsFileSystem.close();
    }
}
