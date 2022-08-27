package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.fail;

public class ExpTsFileWriter {
  public enum DatasetConstant {
    TIANYUAN(
        100_0002,
        "D:\\iotdb\\tsfile\\src\\test\\resources\\root.tianyuan.d0.s9.csv",
        0.0,
        34.74596333,
        "root.tianyuan.d0",
        "s9");

    public static final long startTime = 0L;
    public static final long interval = 1000;

    // 数据集规模，便于后续查询观察
    public final int totalNumber;
    // CSV文件列表
    public final String csvFilePath;
    // 数据极值
    public final double minValue;
    public final double maxValue;
    // 时间序列路径
    public final String device;
    public final String measurement;

    DatasetConstant(
        int totalNumber,
        String csvFilePath,
        double minValue,
        double maxValue,
        String device,
        String measurement) {
      this.totalNumber = totalNumber;
      this.csvFilePath = csvFilePath;
      this.minValue = minValue;
      this.maxValue = maxValue;
      this.device = device;
      this.measurement = measurement;
    }
  }

  // 文件内的结构设计
  static int pointNumberInPage = 60;
  static int pageNumberInChunk = 10;
  static int chunkNumberInFile = -1;
  // 预计算bucket宽度
  static double bucketWidth = 5.0;

  static DatasetConstant dataset = DatasetConstant.TIANYUAN;

  // 时间序列路径和编码压缩等详细信息
  static String originalDevice = dataset.device;
  static MeasurementSchema originalMeasurementSchema =
      new MeasurementSchema(
          dataset.measurement, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
  static String bucketDevice = String.format("%s.%sb", dataset.device, dataset.measurement);
  static int bucketNumber =
      (int) Math.ceil(dataset.maxValue / bucketWidth) - (int) (dataset.minValue / bucketWidth);
  static List<MeasurementSchema> bucketMeasurementSchemaList = new ArrayList<>(bucketNumber);

  static {
    for (int i = (int) (dataset.minValue / bucketWidth);
        i < (int) Math.ceil(dataset.maxValue / bucketWidth);
        i++) {
      bucketMeasurementSchemaList.add(
          new MeasurementSchema(
              "b" + i, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY));
    }
  }

  @Test
  public void writeAlignedPage() {
    int tsFileVersion = 0;
    String fileName =
        TsFileGeneratorForTest.getTestTsFilePath(
            String.join(".", Arrays.copyOf(dataset.device.split("\\."), 2)), 0, 0, tsFileVersion);
    TsFileWriter currentTsFileWriter = null;
    try {
      // 确保文件路径已创建
      File file = new File(fileName);
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      // 打开第一份TsFile写入流
      currentTsFileWriter = new TsFileWriter(file);
      // 注册序列
      currentTsFileWriter.registerTimeseries(new Path(originalDevice), originalMeasurementSchema);
      currentTsFileWriter.tryToInitialGroupWriter(originalDevice, false);
      currentTsFileWriter.registerAlignedTimeseries(
          new Path(bucketDevice), bucketMeasurementSchemaList);
      currentTsFileWriter
          .tryToInitialGroupWriter(bucketDevice, true)
          .tryToAddSeriesWriter(bucketMeasurementSchemaList);
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(dataset.csvFilePath))) {
      // 计数器
      int count = 0;
      // 时间戳
      long currentTime = DatasetConstant.startTime;
      // 跳过header
      bufferedReader.readLine();
      // 读取数据
      String currentLine = bufferedReader.readLine();
      while (currentLine != null) {
        double value = Double.parseDouble(currentLine.split(",")[2]);
        // 原始序列值
        currentTsFileWriter.write(
            new TSRecord(currentTime, originalDevice)
                .addTuple(new DoubleDataPoint(dataset.measurement, value)));
        // bucket值
        currentTsFileWriter.writeAligned(
            new TSRecord(currentTime, bucketDevice)
                .addTuple(new DoubleDataPoint("b" + (int) (value / bucketWidth), value)));
        // 计数
        count++;
        // 更新时间戳
        currentTime += DatasetConstant.interval;
        // 读取下一行
        currentLine = bufferedReader.readLine();
        if (count % pointNumberInPage == 0) {
          if (count % (pointNumberInPage * pageNumberInChunk) == 0) {
            if (chunkNumberInFile > 0
                && count % (pointNumberInPage * pageNumberInChunk * chunkNumberInFile) == 0) {
              // 封口当前TsFile
              currentTsFileWriter.close();
              // 重定向到新TsFile
              tsFileVersion++;
              currentTsFileWriter =
                  new TsFileWriter(
                      new File(
                          TsFileGeneratorForTest.getTestTsFilePath(
                              String.join(".", Arrays.copyOf(dataset.device.split("\\."), 2)),
                              0,
                              0,
                              tsFileVersion)));
              // 注册序列
              currentTsFileWriter.registerTimeseries(
                  new Path(originalDevice), originalMeasurementSchema);
              currentTsFileWriter.tryToInitialGroupWriter(originalDevice, false);
              currentTsFileWriter.registerAlignedTimeseries(
                  new Path(bucketDevice), bucketMeasurementSchemaList);
              currentTsFileWriter
                  .tryToInitialGroupWriter(bucketDevice, true)
                  .tryToAddSeriesWriter(bucketMeasurementSchemaList);
            } else {
              // 封口当前Chunk
              currentTsFileWriter.flushAllChunkGroups();
              currentTsFileWriter.tryToInitialGroupWriter(originalDevice, false);
              currentTsFileWriter
                  .tryToInitialGroupWriter(bucketDevice, true)
                  .tryToAddSeriesWriter(bucketMeasurementSchemaList);
            }
          } else {
            // 封口当前Page
            for (IChunkGroupWriter chunkGroupWriter :
                currentTsFileWriter.getGroupWriters().values()) {
              chunkGroupWriter.sealAllChunks();
            }
          }
        }
      }
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      try {
        currentTsFileWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void writeNonAlignedPage() {
    int tsFileVersion = 0;
    String fileName =
        TsFileGeneratorForTest.getTestTsFilePath(
            String.join(".", Arrays.copyOf(dataset.device.split("\\."), 2)), 1, 0, tsFileVersion);
    TsFileWriter currentTsFileWriter = null;
    try {
      // 确保文件路径已创建
      File file = new File(fileName);
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      // 打开第一份TsFile写入流
      currentTsFileWriter = new TsFileWriter(file);
      // 注册序列
      currentTsFileWriter.registerTimeseries(new Path(originalDevice), originalMeasurementSchema);
      currentTsFileWriter.registerTimeseries(new Path(bucketDevice), bucketMeasurementSchemaList);
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(dataset.csvFilePath))) {
      // 计数器
      int count = 0;
      // 时间戳
      long currentTime = DatasetConstant.startTime;
      // 跳过header
      bufferedReader.readLine();
      // 读取数据
      String currentLine = bufferedReader.readLine();
      while (currentLine != null) {
        double value = Double.parseDouble(currentLine.split(",")[2]);
        // 原始序列值
        currentTsFileWriter.write(
            new TSRecord(currentTime, originalDevice)
                .addTuple(new DoubleDataPoint(dataset.measurement, value)));
        // bucket值
        currentTsFileWriter.write(
            new TSRecord(currentTime, bucketDevice)
                .addTuple(new DoubleDataPoint("b" + (int) (value / bucketWidth), value)));
        // 计数
        count++;
        // 更新时间戳
        currentTime += DatasetConstant.interval;
        // 读取下一行
        currentLine = bufferedReader.readLine();
        if (count % pointNumberInPage == 0) {
          if (count % (pointNumberInPage * pageNumberInChunk) == 0) {
            if (chunkNumberInFile > 0
                && count % (pointNumberInPage * pageNumberInChunk * chunkNumberInFile) == 0) {
              // 封口当前TsFile
              currentTsFileWriter.close();
              // 重定向到新TsFile
              tsFileVersion++;
              currentTsFileWriter =
                  new TsFileWriter(
                      new File(
                          TsFileGeneratorForTest.getTestTsFilePath(
                              String.join(".", Arrays.copyOf(dataset.device.split("\\."), 2)),
                              1,
                              0,
                              tsFileVersion)));
              // 注册序列
              currentTsFileWriter.registerTimeseries(
                  new Path(originalDevice), originalMeasurementSchema);
              currentTsFileWriter.registerTimeseries(
                  new Path(bucketDevice), bucketMeasurementSchemaList);
            } else {
              // 封口当前Chunk
              currentTsFileWriter.flushAllChunkGroups();
            }
          } else {
            // 封口当前Page
            for (IChunkGroupWriter chunkGroupWriter :
                currentTsFileWriter.getGroupWriters().values()) {
              chunkGroupWriter.sealAllChunks();
            }
          }
        }
      }
    } catch (IOException | WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      try {
        currentTsFileWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }
}
