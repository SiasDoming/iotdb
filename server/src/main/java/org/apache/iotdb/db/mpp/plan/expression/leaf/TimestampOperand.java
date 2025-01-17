/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.plan.expression.leaf;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimestampOperand extends LeafOperand {

  public static final PartialPath TIMESTAMP_PARTIAL_PATH = new PartialPath("Time", false);

  public TimestampOperand() {
    // do nothing
  }

  public TimestampOperand(ByteBuffer byteBuffer) {
    // do nothing
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitTimeStampOperand(this, context);
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return true;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    resultExpressions.add(this);
  }

  @Override
  public void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    resultExpressions.add(this);
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    pathSet.add(TIMESTAMP_PARTIAL_PATH);
  }

  @Override
  public TSDataType inferTypes(TypeProvider typeProvider) {
    typeProvider.setType(toString(), TSDataType.INT64);
    return TSDataType.INT64;
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    // do nothing
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    // do nothing
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  protected boolean isConstantOperandInternal() {
    return false;
  }

  @Override
  protected String getExpressionStringInternal() {
    return "Time";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.TIMESTAMP;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    // do nothing
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    // do nothing
  }
}
