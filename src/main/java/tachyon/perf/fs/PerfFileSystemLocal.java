package tachyon.perf.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This only works for local test.
 */
public class PerfFileSystemLocal extends PerfFileSystem {
  public static PerfFileSystem getClient() {
    return new PerfFileSystemLocal();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public OutputStream create(String path) throws IOException {
    File file = new File(path);
    File parent = file.getParentFile();
    if (parent != null) {
      if (!parent.exists()) {
        parent.mkdirs();
      }
    }
    return new FileOutputStream(path);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    return create(path);
  }

  @Override
  public boolean createEmptyFile(String path) throws IOException {
    File file = new File(path);
    File parent = file.getParentFile();
    if (parent != null) {
      if (!parent.exists()) {
        parent.mkdirs();
      }
    }
    new FileOutputStream(file).close();
    return true;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    File file = new File(path);
    boolean success = true;
    if (recursive && file.isDirectory()) {
      String[] files = file.list();
      for (String child : files) {
        success = success && delete(path + "/" + child, true);
      }
    }
    return success && file.delete();
  }

  @Override
  public boolean exists(String path) throws IOException {
    return new File(path).exists();
  }

  @Override
  public long getLength(String path) throws IOException {
    return new File(path).length();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return new File(path).isDirectory();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return new File(path).isFile();
  }

  @Override
  public List<String> listFullPath(String path) throws IOException {
    File file = new File(path);
    if (file.isFile()) {
      ArrayList<String> ret = new ArrayList<String>(1);
      ret.add(file.getAbsolutePath());
      return ret;
    } else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        ArrayList<String> ret = new ArrayList<String>(files.length);
        for (File child : files) {
          ret.add(child.getAbsolutePath());
        }
        return ret;
      }
    }
    return null;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    File file = new File(path);
    return (createParent ? file.mkdirs() : file.mkdir());
  }

  @Override
  public InputStream open(String path) throws IOException {
    return new FileInputStream(path);
  }

  @Override
  public InputStream open(String path, String readType) throws IOException {
    return open(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    File srcFile = new File(src);
    File dstFile = new File(dst);
    File pFile = dstFile.getParentFile();
    if (pFile != null) {
      pFile.mkdirs();
    }
    return srcFile.renameTo(dstFile);
  }
}
