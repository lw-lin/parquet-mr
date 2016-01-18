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
package org.apache.parquet.io;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompatSchemaRebuilderV2;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory constructing the ColumnIO structure from the schema
 *
 * @author Julien Le Dem
 */
public class ColumnIOFactory {

  private class ColumnIOCreatorVisitor implements TypeVisitor {

    private MessageColumnIO columnIO;
    private GroupColumnIO current;
    private List<PrimitiveColumnIO> leaves = new ArrayList<PrimitiveColumnIO>();
    private final boolean validating;
    private final MessageType requestedSchema;
    private final FilterCompat.FilterPredicateCompat filter;
    private final MessageType filterSchema;
    private final String createdBy;

    private PredefinedColumnIOSet currentColumnIOSet;
    private int currentRequestedIndex;
    private Type currentRequestedType;
    private final int currentFilterIndex = -100;
    private Type currentFilterType;

    private boolean strictTypeChecking;

    private ColumnIOCreatorVisitor(boolean validating, MessageType requestedSchema, FilterCompat.FilterPredicateCompat filter, String createdBy,
                                   boolean strictTypeChecking) {
      this.validating = validating;
      this.requestedSchema = requestedSchema;
      this.filter = filter;
      // collects the columns referred by filter
      this.filterSchema = filter == null ? null : FilterCompatSchemaRebuilderV2.INSTANCE.rebuildSchema(filter);
      this.createdBy = createdBy;
      this.strictTypeChecking = strictTypeChecking;
    }

    @Override
    public void visit(MessageType messageType) {
      columnIO = new MessageColumnIO(requestedSchema, validating, createdBy);
      visitChildren(columnIO, messageType, requestedSchema, filterSchema);
      columnIO.setLevels();
      columnIO.setLeaves(leaves);
    }

    @Override
    public void visit(GroupType groupType) {
      if (currentRequestedType.isPrimitive()) {
        incompatibleSchema(groupType, currentRequestedType);
      }
      GroupColumnIO newIO;
      switch (currentColumnIOSet) {
        case D_IN_FILE_AND_REQUESTED_BUT_NOT_IN_FILTER:
          newIO = new GroupColumnIO(groupType, current, currentRequestedIndex, currentColumnIOSet);
          current.add(newIO);
          visitChildren(newIO, groupType, currentRequestedType.asGroupType(), null);
          break;
        case E_IN_FILE_AND_FILTER_BUT_NOT_IN_REQUESTED:
          newIO = new GroupColumnIO(groupType, current, currentFilterIndex, currentColumnIOSet);
          current.add(newIO);
          visitChildren(newIO, groupType, null, currentFilterType.asGroupType());
          break;
        case G_IN_FILE_AND_REQUEST_AND_FILER:
          newIO = new GroupColumnIO(groupType, current, currentRequestedIndex, currentColumnIOSet);
          current.add(newIO);
          visitChildren(newIO, groupType, currentRequestedType.asGroupType(), currentFilterType.asGroupType());
          break;
        default:
          throw new ShouldNeverHappenException();
      }
    }

    private void visitChildren(GroupColumnIO newIO, GroupType groupType, GroupType requestedGroupType, GroupType filterGroupType) {
      GroupColumnIO oldIO = current;
      current = newIO;
      for (Type type : groupType.getFields()) {
        boolean inFileSchema = true;
        boolean inRequestedSchema = requestedGroupType != null && requestedGroupType.containsField(type.getName());
        boolean inFilterSchema = filterGroupType != null && filterGroupType.containsField(type.getName());
        currentColumnIOSet = PredefinedColumnIOSet.get(inFileSchema, inRequestedSchema, inFilterSchema);

        // if the file schema does not contain the field it will just stay null
        if (inRequestedSchema) {
          currentRequestedIndex = requestedGroupType.getFieldIndex(type.getName());
          currentRequestedType = requestedGroupType.getType(currentRequestedIndex);
          if (currentRequestedType.getRepetition().isMoreRestrictiveThan(type.getRepetition())) {
            incompatibleSchema(type, currentRequestedType);
          }
        }
        if (inFilterSchema) {
          currentFilterType = filterGroupType.getType(type.getName());
        }
        type.accept(this);
      }
      current = oldIO;
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
      PrimitiveColumnIO newIO;
      switch (currentColumnIOSet) {
        case D_IN_FILE_AND_REQUESTED_BUT_NOT_IN_FILTER:
        case G_IN_FILE_AND_REQUEST_AND_FILER:
          if (!currentRequestedType.isPrimitive() ||
              (this.strictTypeChecking && currentRequestedType.asPrimitiveType().getPrimitiveTypeName() != primitiveType.getPrimitiveTypeName())) {
            incompatibleSchema(primitiveType, currentRequestedType);
          }
          newIO = new PrimitiveColumnIO(primitiveType, current, currentRequestedIndex, leaves.size(), currentColumnIOSet);
          break;
        case E_IN_FILE_AND_FILTER_BUT_NOT_IN_REQUESTED:
          newIO = new PrimitiveColumnIO(primitiveType, current, currentFilterIndex, leaves.size(), currentColumnIOSet);
          break;
        default:
          throw new ShouldNeverHappenException();
      }

      current.add(newIO);
      leaves.add(newIO);
    }

    private void incompatibleSchema(Type fileType, Type requestedType) {
      throw new ParquetDecodingException("The requested schema is not compatible with the file schema. incompatible types: " + requestedType + " != " + fileType);
    }

    public MessageColumnIO getColumnIO() {
      return columnIO;
    }

  }

  private final String createdBy;
  private final boolean validating;

  /**
   * validation is off by default
   */
  public ColumnIOFactory() {
    this(null, false);
  }

  /**
   * validation is off by default
   *
   * @param createdBy createdBy string for readers
   */
  public ColumnIOFactory(String createdBy) {
    this(createdBy, false);
  }

  /**
   * @param validating to turn validation on
   */
  public ColumnIOFactory(boolean validating) {
    this(null, validating);
  }

  /**
   * @param createdBy  createdBy string for readers
   * @param validating to turn validation on
   */
  public ColumnIOFactory(String createdBy, boolean validating) {
    super();
    this.createdBy = createdBy;
    this.validating = validating;
  }

  /**
   * @param requestedSchema the requestedSchema we want to read/write
   * @param fileSchema      the file schema (when reading it can be different from the requested schema)
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema) {
    return getColumnIO(requestedSchema, fileSchema, true);
  }

  /**
   * @param requestedSchema the requestedSchema we want to read/write
   * @param fileSchema      the file schema (when reading it can be different from the requested schema)
   * @param strict          should file type and requested primitive types match
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema, boolean strict) {
    return getColumnIO(requestedSchema, fileSchema, null, strict);
  }


  /**
   * @param requestedSchema the requestedSchema we want to read/write
   * @param fileSchema      the file schema (when reading it can be different from the requested schema)
   * @param filter          the predicate should be applied (it can contains columns specified neither in requestedSchema nor in fileSchema)
   * @param strict          should file type and requested primitive types match
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema, FilterCompat.FilterPredicateCompat filter, boolean strict) {
    ColumnIOCreatorVisitor visitor = new ColumnIOCreatorVisitor(validating, requestedSchema, filter, createdBy, strict);
    fileSchema.accept(visitor);
    return visitor.getColumnIO();
  }

  /**
   * @param schema the schema we want to read/write
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType schema) {
    return this.getColumnIO(schema, schema);
  }

}
