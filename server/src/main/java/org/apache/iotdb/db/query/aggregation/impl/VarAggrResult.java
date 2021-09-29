package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.*;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class VarAggrResult extends AggregateResult {

  private TSDataType seriesDataType;
  private double squareSum = 0.0;
  private double sum = 0.0;
  private long cnt = 0;

  public VarAggrResult(TSDataType seriesDataType) {
    super(TSDataType.DOUBLE, AggregationType.VAR);
    this.seriesDataType = seriesDataType;
    reset();
  }

  @Override
  public Double getResult() {
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) throws QueryProcessException {
    // skip empty statistics
    if (statistics.getCount() == 0) {
      return;
    }
    // update intermediate results from statistics
    if (statistics instanceof BooleanStatistics || statistics instanceof IntegerStatistics) {
      squareSum += statistics.getSquareSumFloatValue();
      sum += statistics.getSumLongValue();
    } else if (statistics instanceof LongStatistics
        || statistics instanceof FloatStatistics
        || statistics instanceof DoubleStatistics) {
      squareSum += statistics.getSquareSumDoubleValue();
      sum += statistics.getSumDoubleValue();
    } else {
      throw new StatisticsClassException("Binary statistics does not support: var");
    }
    cnt += statistics.getCount();
    setDoubleValue(squareSum / cnt - sum * sum / cnt / cnt);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage)
      throws IOException, QueryProcessException {
    updateResultFromPageData(dataInThisPage, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound)
      throws IOException {
    while (dataInThisPage.hasCurrent()) {
      if (dataInThisPage.currentTime() >= maxBound || dataInThisPage.currentTime() < minBound) {
        break;
      }
      updateVar(dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateVar(values[i]);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateVar(values[i]);
      }
    }
  }

  private void updateVar(Object varVal) throws UnSupportedDataTypeException {
    double val;
    switch (seriesDataType) {
      case INT32:
        val = (int) varVal;
        break;
      case INT64:
        val = (long) varVal;
        break;
      case FLOAT:
        val = (float) varVal;
        break;
      case DOUBLE:
        val = (double) varVal;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation VAR : %s", seriesDataType));
    }
    squareSum += val * val;
    sum += val;
    cnt++;
    setDoubleValue(squareSum / cnt - sum * sum / cnt / cnt);
  }

  public void setVarResult(TSDataType type, Object val) throws UnSupportedDataTypeException {
    cnt = 1;
    switch (type) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        squareSum = (double) val * (double) val;
        sum = (double) val;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation VAR : %s", type));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    VarAggrResult anotherVar = (VarAggrResult) another;
    // skip empty results
    if (anotherVar.getCnt() == 0) {
      return;
    }
    squareSum += anotherVar.getSquareSum();
    sum += anotherVar.getSum();
    cnt++;
    setDoubleValue(squareSum / cnt - sum * sum / cnt / cnt);
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    this.squareSum = buffer.getDouble();
    this.sum = buffer.getDouble();
    this.cnt = buffer.getLong();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    ReadWriteIOUtils.write(squareSum, outputStream);
    ReadWriteIOUtils.write(sum, outputStream);
    ReadWriteIOUtils.write(cnt, outputStream);
  }

  @Override
  public void reset() {
    super.reset();
    squareSum = 0.0;
    sum = 0.0;
    cnt = 0L;
  }

  public double getSquareSum() {
    return squareSum;
  }

  public double getSum() {
    return sum;
  }

  public long getCnt() {
    return cnt;
  }
}
