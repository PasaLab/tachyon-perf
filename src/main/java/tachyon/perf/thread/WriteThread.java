package tachyon.perf.thread;

import java.io.IOException;
import java.util.List;

import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.perf.conf.PerfConf;

public class WriteThread extends PerfThread {
  private static final int DEFALUT_WRITE_GRAIN = 4194304;// 4MB

  private byte[] mContent;
  private long mFileLength;
  private String mTfsAddress;
  private List<String> mWriteFileList;
  private WriteType mWriteType;

  public WriteThread(int id, List<String> writeFileList, WriteType writeType) {
    super(id);
    mThreadReport = new RWThreadReport();

    mContent = new byte[DEFALUT_WRITE_GRAIN];
    PerfConf perfConf = PerfConf.get();
    mFileLength = perfConf.FILE_LENGTH;
    mTfsAddress = perfConf.TFS_ADDRESS;
    mWriteFileList = writeFileList;
    mWriteType = writeType;
    // TODO: content initialize?
  }

  @Override
  public void run() {
    mThreadReport.setStartTimeMs(System.currentTimeMillis());
    TachyonFS tachyonClient;
    try {
      tachyonClient = TachyonFS.get(mTfsAddress);
    } catch (IOException e) {
      LOG.error("Write Thread " + ID + " falied to connect Tachyon");
      throw new RuntimeException(e);
    }

    mThreadReport.setSuccess(true);
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
          ((RWThreadReport) mThreadReport).addSuccessBytes(DEFALUT_WRITE_GRAIN);
          remainLength -= DEFALUT_WRITE_GRAIN;
        }
        if (remainLength > 0) {
          os.write(mContent, 0, (int) remainLength);
          ((RWThreadReport) mThreadReport).addSuccessBytes(remainLength);
        }
        os.close();
      } catch (IOException e) {
        LOG.error("Write thread " + ID + "failed to write file " + fileName, e);
        mThreadReport.setSuccess(false);
        break;
      }
      ((RWThreadReport) mThreadReport).addSuccessFiles(1);
    }
    mThreadReport.setFinishTimeMs(System.currentTimeMillis());
  }
}
