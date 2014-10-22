package tachyon.perf.benchmark.iterate;

import java.io.IOException;
import java.util.List;

import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.benchmark.Operators;

public class Iterate2Thread extends IterateThread {
  @Override
  public void run() {
    long readBytes = 0;
    long readTimeMs = 0;
    long writeBytes = 0;
    long writeTimeMs = 0;
    mSuccess = true;

    String dataDir;
    if (mShuffle) {
      dataDir = mWorkDir + "/data";
    } else {
      dataDir = mWorkDir + "/data/" + mTaskId;
    }
    long tTimeMs = System.currentTimeMillis();
    for (int w = 0; w < mWriteFilesNum; w ++) {
      try {
        String fileName = mTaskId + "-" + mId + "-" + w;
        Operators.writeSingleFile(mFileSystem, dataDir + "/" + fileName, mFileLength, mBlockSize,
            mBufferSize, mWriteType);
        // writeBytes += mFileLength;
      } catch (IOException e) {
        LOG.error("Failed to write file", e);
        mSuccess = false;
        break;
      }
    }
    // tTimeMs = System.currentTimeMillis() - tTimeMs;
    // writeTimeMs += tTimeMs;

    String smallFilePath = null;
    for (int i = 0; i < mIterations; i ++) {
      try {
        syncBarrier(i);
      } catch (IOException e) {
        LOG.error("Error in Sync Barrier", e);
        mSuccess = false;
      }
      tTimeMs = System.currentTimeMillis();
      try {
        if (smallFilePath != null) {
          readBytes += Operators.readSingleFile(mFileSystem, smallFilePath, mBufferSize, mReadType);
        }
        List<String> candidates = mFileSystem.listFullPath(dataDir);
        List<String> readList = ListGenerator.generateRandomReadFiles(mReadFilesNum, candidates);
        for (String fileName : readList) {
          readBytes += Operators.readSingleFile(mFileSystem, fileName, mBufferSize, mReadType);
        }
      } catch (Exception e) {
        LOG.error("Failed to read file", e);
        mSuccess = false;
      }
      tTimeMs = System.currentTimeMillis() - tTimeMs;
      readTimeMs += tTimeMs;

      smallFilePath = mWorkDir + "/small-data/" + mTaskId + "-" + mId + "-" + i;
      tTimeMs = System.currentTimeMillis();
      try {
        Operators.writeSingleFile(mFileSystem, smallFilePath, mFileLength, mBlockSize, mBufferSize,
            mWriteType);
        writeBytes += mFileLength;
      } catch (IOException e) {
        LOG.error("Failed to write file", e);
        mSuccess = false;
      }
      tTimeMs = System.currentTimeMillis() - tTimeMs;
      writeTimeMs += tTimeMs;
    }
    mReadThroughput = (readBytes / 1024.0 / 1024.0) / (readTimeMs / 1000.0);
    mWriteThroughput = (writeBytes / 1024.0 / 1024.0) / (writeTimeMs / 1000.0);
  }
}
