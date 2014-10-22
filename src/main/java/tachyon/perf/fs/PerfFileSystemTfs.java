package tachyon.perf.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Throwables;

import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.thrift.ClientFileInfo;

public class PerfFileSystemTfs extends PerfFileSystem {
  public static PerfFileSystem getClient(String path) {
    return new PerfFileSystemTfs(path);
  }

  private TachyonFS mTfs;

  private PerfFileSystemTfs(String path) {
    try {
      mTfs = TachyonFS.get(new TachyonURI(path));
    } catch (IOException e) {
      LOG.error("Failed to get TachyonFS", e);
      Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    mTfs.close();
  }

  @Override
  public OutputStream create(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      mTfs.createFile(uri);
    }
    return mTfs.getFile(uri).getOutStream(WriteType.TRY_CACHE);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      mTfs.createFile(uri, blockSizeByte);
    }
    return mTfs.getFile(uri).getOutStream(WriteType.TRY_CACHE);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    WriteType type = WriteType.valueOf(writeType);
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      mTfs.createFile(uri, blockSizeByte);
    }
    return mTfs.getFile(uri).getOutStream(type);
  }

  @Override
  public boolean createEmptyFile(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (mTfs.exist(uri)) {
      return false;
    }
    return (mTfs.createFile(uri) != -1);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    return mTfs.delete(new TachyonURI(path), recursive);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mTfs.exist(new TachyonURI(path));
  }

  @Override
  public long getLength(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      return 0;
    }
    return mTfs.getFile(uri).length();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      return false;
    }
    return mTfs.getFile(uri).isDirectory();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      return false;
    }
    return mTfs.getFile(uri).isFile();
  }

  @Override
  public List<String> listFullPath(String path) throws IOException {
    List<ClientFileInfo> files = mTfs.listStatus(new TachyonURI(path));
    if (files == null) {
      return null;
    }
    ArrayList<String> ret = new ArrayList<String>(files.size());
    for (ClientFileInfo fileInfo : files) {
      ret.add(fileInfo.path);
    }
    return ret;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (mTfs.exist(uri)) {
      return false;
    }
    return mTfs.mkdirs(uri, createParent);
  }

  @Override
  public InputStream open(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.getFile(uri).getInStream(ReadType.NO_CACHE);
  }

  @Override
  public InputStream open(String path, String readType) throws IOException {
    ReadType type = ReadType.valueOf(readType);
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.getFile(uri).getInStream(type);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    TachyonURI srcURI = new TachyonURI(src);
    TachyonURI dstURI = new TachyonURI(dst);
    return mTfs.rename(srcURI, dstURI);
  }

}
