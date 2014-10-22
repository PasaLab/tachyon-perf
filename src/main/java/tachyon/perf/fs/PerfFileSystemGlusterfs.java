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

public class PerfFileSystemGlusterfs extends PerfFileSystem {
  private static final int MAX_TRY = 5;

  public static PerfFileSystem getClient(String path, String glusterfsImpl,
      String glusterfsVolumes, String glusterfsMounts) {
    return new PerfFileSystemGlusterfs(path, glusterfsImpl, glusterfsVolumes, glusterfsMounts);
  }

  private FileSystem mGlusterfs;

  private PerfFileSystemGlusterfs(String path, String glusterfsImpl, String glusterfsVolumes,
      String glusterfsMounts) {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", path);
    conf.set("fs.glusterfs.impl", glusterfsImpl);
    conf.set("mapred.system.dir", "glusterfs:///mapred/system");
    conf.set("fs.glusterfs.volumes", glusterfsVolumes);
    conf.set("fs.glusterfs.volume.fuse." + glusterfsVolumes, glusterfsMounts);

    try {
      mGlusterfs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error("Failed to get Glusterfs", e);
      Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    mGlusterfs.close();
  }

  @Override
  public OutputStream create(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        FSDataOutputStream os = mGlusterfs.create(new Path(path));
        return os;
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    // Use the default block size of HDFS
    return create(path);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    return create(path, blockSizeByte);
  }

  @Override
  public boolean createEmptyFile(String path) throws IOException {
    OutputStream os = create(path);
    os.close();
    return true;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mGlusterfs.delete(new Path(path), recursive);
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean exists(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mGlusterfs.exists(new Path(path));
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public long getLength(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        Path p = new Path(path);
        if (!mGlusterfs.exists(p)) {
          return 0;
        }
        return mGlusterfs.getFileStatus(p).getLen();
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mGlusterfs.isDirectory(new Path(path));
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mGlusterfs.isFile(new Path(path));
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public List<String> listFullPath(String path) throws IOException {
    if (isFile(path)) {
      ArrayList<String> list = new ArrayList<String>(1);
      list.add(path);
      return list;
    } else if (isDirectory(path)) {
      IOException te = null;
      int cnt = 0;
      while (cnt < MAX_TRY) {
        try {
          FileStatus fs[] = mGlusterfs.listStatus(new Path(path));
          int len = fs.length;
          ArrayList<String> list = new ArrayList<String>(len);
          Path listpath[] = FileUtil.stat2Paths(fs);
          for (int i = 0; i < len; i ++) {
            list.add(listpath[i].toString());
          }
          return list;
        } catch (IOException e) {
          cnt ++;
          te = e;
        }
      }
      throw te;
    }
    return null;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        Path p = new Path(path);
        if (mGlusterfs.exists(p)) {
          return false;
        }
        return mGlusterfs.mkdirs(p);
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public InputStream open(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        Path p = new Path(path);
        if (!mGlusterfs.exists(p)) {
          throw new FileNotFoundException("File not exists " + path);
        }
        return mGlusterfs.open(p);
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }

  @Override
  public InputStream open(String path, String readType) throws IOException {
    return open(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        Path dstParentPath = dstPath.getParent();
        if (!mGlusterfs.exists(dstParentPath)) {
          mGlusterfs.mkdirs(dstParentPath);
        }
        return mGlusterfs.rename(srcPath, dstPath);
      } catch (IOException e) {
        cnt ++;
        te = e;
      }
    }
    throw te;
  }
}
