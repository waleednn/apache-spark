/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.*;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.partitioning.ClusteredDistribution;
import org.apache.spark.sql.connector.read.partitioning.Distribution;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.IOException;
import java.util.Arrays;

public class JavaOrderAndPartitionAwareDataSource extends JavaPartitionAwareDataSource {

  static class MyScanBuilder extends JavaPartitionAwareDataSource.MyScanBuilder implements SupportsReportOrdering {

    @Override
    public InputPartition[] planInputPartitions() {
      InputPartition[] partitions = new InputPartition[2];
      partitions[0] = new SpecificInputPartition(new int[]{1, 1, 2}, new int[]{4, 5, 6});
      partitions[1] = new SpecificInputPartition(new int[]{3, 4, 4}, new int[]{6, 1, 2});
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new SpecificReaderFactory();
    }

    @Override
    public SortOrder[] outputOrdering() {
      return new SortOrder[] { new MySortOrder("i"), new MySortOrder("j") };
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new JavaSimpleBatchTable() {
      @Override
      public Transform[] partitioning() {
        return new Transform[] { Expressions.identity("i") };
      }

      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MyScanBuilder();
      }
    };
  }

  static class MyPartitioning implements Partitioning {

    @Override
    public int numPartitions() {
      return 2;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
      if (distribution instanceof ClusteredDistribution) {
        String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
        return Arrays.asList(clusteredCols).contains("i");
      }

      return false;
    }
  }

  static class MySortOrder implements SortOrder {
    private final Expression expression;

    public MySortOrder(String columnName) {
      this.expression = new MyIdentityTransform(new MyNamedReference(columnName));
    }

    @Override
    public Expression expression() { return expression; }

    @Override
    public SortDirection direction() { return SortDirection.ASCENDING; }

    @Override
    public NullOrdering nullOrdering() { return NullOrdering.NULLS_FIRST; }
  }

  static class MyNamedReference implements NamedReference {
    private final String[] parts;

    public MyNamedReference(String part) { this.parts = new String[] { part }; }

    @Override
    public String[] fieldNames() { return this.parts; }
  }

  static class MyIdentityTransform implements Transform {
    private final Expression[] args;

    public MyIdentityTransform(NamedReference namedReference) {
      this.args = new Expression[] { namedReference };
    }

    @Override
    public String name() { return "identity"; }

    @Override
    public NamedReference[] references() { return new NamedReference[0]; }

    @Override
    public Expression[] arguments() { return this.args; }
  }

}
