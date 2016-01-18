/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.filter2.compat;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.HashSet;


public class FilterCompatSchemaRebuilder implements FilterCompat.Visitor<MessageType> {

  public static final FilterCompatSchemaRebuilder INSTANCE = new FilterCompatSchemaRebuilder();

  private FilterCompatSchemaRebuilder() {
  }

  @Override
  public MessageType visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
    FilterPredicateSchemaRebuilder rebuilder = new FilterPredicateSchemaRebuilder();
    filterPredicateCompat.getFilterPredicate().accept(rebuilder);
    return rebuilder.getMessageType();
  }

  @Override
  public MessageType visit(FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
    throw new ParquetRuntimeException("Not supported yet") {
    };
  }

  @Override
  public MessageType visit(FilterCompat.NoOpFilter noOpFilter) {
    return null;
  }

  /**
   * This class is stateful, and not thread-safe So create an instance every time this is used
   */
  private static class FilterPredicateSchemaRebuilder implements FilterPredicate.Visitor<Void> {

    private final HashSet<ColumnPath> columnSet = new HashSet<ColumnPath>();
    private final MessageType messageType = new MessageType("root");

    @Override
    public <T extends Comparable<T>> Void visit(Operators.Eq<T> eq) {
      columnSet.add(eq.getColumn().getColumnPath());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.NotEq<T> notEq) {
      columnSet.add(notEq.getColumn().getColumnPath());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.Lt<T> lt) {
      columnSet.add(lt.getColumn().getColumnPath());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.LtEq<T> ltEq) {
      columnSet.add(ltEq.getColumn().getColumnPath());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.Gt<T> gt) {
      columnSet.add(gt.getColumn().getColumnPath());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.GtEq<T> gtEq) {
      columnSet.add(gtEq.getColumn().getColumnPath());
      return null;
    }

    @Override
    public Void visit(Operators.And and) {
      and.getLeft().accept(this);
      and.getRight().accept(this);
      return null;
    }

    @Override
    public Void visit(Operators.Or or) {
      or.getLeft().accept(this);
      or.getRight().accept(this);
      return null;
    }

    @Override
    public Void visit(Operators.Not not) {
      not.getPredicate().accept(this);
      return null;
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(
        Operators.UserDefined<T, U> udp) {
      columnSet.add(udp.getColumn().getColumnPath());
      return null;
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(
        Operators.LogicalNotUserDefined<T, U> udp) {

      udp.getUserDefined().accept(this);
      return null;
    }

    public MessageType getMessageType() {
      return messageType;
    }
  }
}