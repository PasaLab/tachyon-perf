package tachyon.perf.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

public abstract class PerfFileSystem {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static PerfFileSystem get(String path) throws IOException {
    if (isGlusterfs(path)) {
      PerfConf perfConf = PerfConf.get();
      return PerfFileSystemGlusterfs.getClient(path, perfConf.GLUSTERFS_IMPL,
          perfConf.GLUSTERFS_VOLUMES, perfConf.GLUSTERFS_MOUNTS);
    } else if (isHdfs(path)) {
      return PerfFileSystemHdfs.getClient(path, PerfConf.get().HDFS_IMPL);
    } else if (isLocalFS(path)) {
      return PerfFileSystemLocal.getClient();
    } else if (isTfs(path)) {
      return PerfFileSystemTfs.getClient(path);
    }
    throw new IOException("Unknown file system scheme " + path);
  }

  private static boolean isGlusterfs(final String path) {
    for (final String prefix : PerfConf.get().GLUSTER_PREFIX) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isHdfs(final String path) {
    for (final String prefix : PerfConf.get().HDFS_PREFIX) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isLocalFS(final String path) {
    for (final String prefix : PerfConf.get().LFS_PREFIX) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isTfs(final String path) {
    for (final String prefix : PerfConf.get().TFS_PREFIX) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Close the connection to the file system
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * Create a file. Use the default block size and write type if supported.
   * 
   * @param path the file's full path
   * @return the output stream of the created file
   * @throws IOException
   */
  public abstract OutputStream create(String path) throws IOException;

  /**
   * Create a file with the specified block size. Use the default write type if supported.
   * 
   * @param path the file's full path
   * @param blockSizeByte the block size of the file
   * @return the output stream of the created file
   * @throws IOException
   */
  public abstract OutputStream create(String path, int blockSizeByte) throws IOException;

  /**
   * Create a file with the specified block size and write type.
   * 
   * @param path the file's full path
   * @param blockSizeByte the block size of the file
   * @param writeType the write type of the file
   * @return the output stream of the created file
   * @throws IOException
   */
  public abstract OutputStream create(String path, int blockSizeByte, String writeType)
      throws IOException;

  /**
   * Create an empty file
   * 
   * @param path the file's full path
   * @return true if success, false otherwise.
   * @throws IOException
   */
  public abstract boolean createEmptyFile(String path) throws IOException;

  /**
   * Delete the file. If recursive and the path is a directory, it will delete all the files under
   * the path.
   * 
   * @param path the file's full path
   * @param recursive It true, delete recursively
   * @return true if success, false otherwise.
   * @throws IOException
   */
  public abstract boolean delete(String path, boolean recursive) throws IOException;

  /**
   * Check whether the file exists or not.
   * 
   * @param path the file's full path
   * @return true if exists, false otherwise
   * @throws IOException
   */
  public abstract boolean exists(String path) throws IOException;

  /**
   * Get the length of the file, in bytes.
   * 
   * @param path
   * @return the length of the file in bytes
   * @throws IOException
   */
  public abstract long getLength(String path) throws IOException;

  /**
   * Check if the path is a directory.
   * 
   * @param path the file's full path
   * @return true if it's a directory, false otherwise
   * @throws IOException
   */
  public abstract boolean isDirectory(String path) throws IOException;

  /**
   * Check if the path is a file.
   * 
   * @param path the file's full path
   * @return true if it's a file, false otherwise
   * @throws IOException
   */
  public abstract boolean isFile(String path) throws IOException;

  /**
   * List the files under the path. If the path is a file, return the full path of the file. if the
   * path is a directory, return the full paths of all the files under the path. Otherwise return
   * null.
   * 
   * @param path the file's full path
   * @return the list contains the full paths of the listed files
   * @throws IOException
   */
  public abstract List<String> listFullPath(String path) throws IOException;

  /**
   * Creates the directory named by the path. If the folder already exists, the method returns
   * false.
   * 
   * @param path the file's full path
   * @param createParent If true, the method creates any necessary but nonexistent parent
   *        directories. Otherwise, the method does not create nonexistent parent directories.
   * @return true if success, false otherwise
   * @throws IOException
   */
  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  /**
   * Open a file and return it's input stream. Use the default read type if supported.
   * 
   * @param path the file's full path
   * @return the input stream of the opened file
   * @throws IOException
   */
  public abstract InputStream open(String path) throws IOException;

  /**
   * Open a file and return it's input stream, with the specified read type.
   * 
   * @param path the file's full path
   * @param readType the read type of the file
   * @return the input stream of the opened file
   * @throws IOException
   */
  public abstract InputStream open(String path, String readType) throws IOException;

  /**
   * Rename the file.
   * 
   * @param src the src full path
   * @param dst the dst full path
   * @return true if success, false otherwise
   * @throws IOException
   */
  public abstract boolean rename(String src, String dst) throws IOException;
}
