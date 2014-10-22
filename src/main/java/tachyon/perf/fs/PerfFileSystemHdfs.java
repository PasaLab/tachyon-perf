package tachyon.perf.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Throwables;


public class PerfFileSystemHdfs extends PerfFileSystem {
  public static PerfFileSystem getClient(String path, String hdfsImpl) {
    return new PerfFileSystemHdfs(path, hdfsImpl);
  }

  private FileSystem mHdfs;

  private PerfFileSystemHdfs(String path, String hdfsImpl) {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", path);
    conf.set("fs.hdfs.impl", hdfsImpl);

    // To disable the instance cache for hdfs client, otherwise it causes the FileSystem closed
    // exception.
    conf.set("fs.hdfs.impl.disable.cache", System.getProperty("fs.hdfs.impl.disable.cache", "true"));

    try {
      mHdfs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error("Failed to get HDFS", e);
      Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    mHdfs.close();
  }

  @Override
  public OutputStream create(String path) throws IOException {
    FSDataOutputStream os = mHdfs.create(new Path(path));
    return os;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    // Use the default block size of HDFS
    FSDataOutputStream os = mHdfs.create(new Path(path));
    return os;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    return create(path, blockSizeByte);
  }

  @Override
  public boolean createEmptyFile(String path) throws IOException {
    FSDataOutputStream os = mHdfs.create(new Path(path));
    os.close();
    return true;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    return mHdfs.delete(new Path(path), recursive);
  }

  @Override
  public boolean exists(String path) throws IOException {

    return mHdfs.exists(new Path(path));
  }

  @Override
  public long getLength(String path) throws IOException {
    Path p = new Path(path);
    if (!mHdfs.exists(p)) {
      return 0;
    }
    return mHdfs.getFileStatus(p).getLen();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mHdfs.isDirectory(new Path(path));

  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mHdfs.isFile(new Path(path));
  }

  @Override
  public List<String> listFullPath(String path) throws IOException {
    if (isFile(path)) {
      ArrayList<String> list = new ArrayList<String>(1);
      list.add(path);
      return list;
    } else if (isDirectory(path)) {
      FileStatus fs[] = mHdfs.listStatus(new Path(path));
      int len = fs.length;
      ArrayList<String> list = new ArrayList<String>(len);
      Path listpath[] = FileUtil.stat2Paths(fs);
      for (int i = 0; i < len; i ++) {
        list.add(listpath[i].toString());
      }
      return list;
    }
    return null;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    Path p = new Path(path);
    if (mHdfs.exists(p)) {
      return false;
    }
    return mHdfs.mkdirs(p);
  }

  @Override
  public InputStream open(String path) throws IOException {
    Path p = new Path(path);
    if (!mHdfs.exists(p)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mHdfs.open(p);
  }

  @Override
  public InputStream open(String path, String readType) throws IOException {
    return open(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);
    Path dstParentPath = dstPath.getParent();
    if (!mHdfs.exists(dstParentPath)) {
      mHdfs.mkdirs(dstParentPath);
    }
    return mHdfs.rename(srcPath, dstPath);
  }
}
