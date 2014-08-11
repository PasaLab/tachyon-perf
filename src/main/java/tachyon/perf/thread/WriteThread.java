package tachyon.perf.thread;

import java.io.IOException;
import java.util.List;

import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

public class WriteThread extends PerfThread {
  private static final int DEFALUT_WRITE_GRAIN = 4194304;// 4MB

  private List<String> mWriteFileList;
  private WriteType mWriteType;
  private long mFileLength;
  private byte[] mContent;

  public WriteThread(int id, String tfsAddress, List<String> writeFileList, WriteType writeType,
      long fileLength) {
    super(id, tfsAddress);
    mWriteFileList = writeFileList;
    mWriteType = writeType;
    mFileLength = fileLength;
    mContent = new byte[DEFALUT_WRITE_GRAIN];
    // TODO: content initialize?
  }

  @Override
  public void run() {
    mThreadReport.start();
    TachyonFS tachyonClient;
    try {
      tachyonClient = TachyonFS.get(mTfsAddress);
    } catch (IOException e) {
      LOG.error("Write Thread " + ID + " falied to connect Tachyon");
      throw new RuntimeException(e);
    }

    for (String fileName : mWriteFileList) {
      try {
        int fileId = -1;
        fileId = tachyonClient.createFile(fileName);
        if (fileId == -1) {
          throw new IOException("Failed to create file " + fileName);
        }

        TachyonFile file = tachyonClient.getFile(fileId);
        OutStream os = file.getOutStream(mWriteType);
        long remainLength = mFileLength;
        while (remainLength >= DEFALUT_WRITE_GRAIN) {
          os.write(mContent);
          mThreadReport.successContent(DEFALUT_WRITE_GRAIN);
          remainLength -= DEFALUT_WRITE_GRAIN;
        }
        if (remainLength > 0) {
          os.write(mContent, 0, (int) remainLength);
          mThreadReport.successContent(remainLength);
        }
        os.close();
      } catch (IOException e) {
        LOG.error("Write thread " + ID + "failed to write file " + fileName, e);
        continue;
      }
      mThreadReport.successFiles(1);
    }
    mThreadReport.end();
  }
}
