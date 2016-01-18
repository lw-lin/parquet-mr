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

enum PredefinedColumnIOSet {

  UNKNOWN(false, false, false, false),
  A_IN_FILE_ONLY(true, true, false, false),
  B_IN_REQUESTED_ONLY(true, false, true, false),
  C_IN_FILTER_ONLY(true, false, false, true),
  D_IN_FILE_AND_REQUESTED_BUT_NOT_IN_FILTER(true, true, true, false),
  E_IN_FILE_AND_FILTER_BUT_NOT_IN_REQUESTED(true, true, false, true),
  F_IN_REQUESTED_AND_FILTER_BUT_NOT_IN_FILE(true, false, true, true),
  G_IN_FILE_AND_REQUEST_AND_FILER(true, true, true, true);

  PredefinedColumnIOSet(boolean known, boolean inFileSchema, boolean inRequestedSchema, boolean inFilterSchema) {
    this.known = known;
    this.inFileSchema = inFileSchema;
    this.inRequestedSchema = inRequestedSchema;
    this.inFilterSchema = inFilterSchema;
  }

  private final boolean known;
  private final boolean inFileSchema;
  private final boolean inRequestedSchema;
  private final boolean inFilterSchema;

  public static PredefinedColumnIOSet get(boolean inFileSchema, boolean inRequestedSchema, boolean inFilterSchema) {
    for (PredefinedColumnIOSet set : values()) {
      if (set.inFileSchema() == inFileSchema && set.inRequestedSchema == inRequestedSchema && set.inFilterSchema() == inFileSchema) {
        return set;
      }
    }
    return UNKNOWN;
  }

  public boolean isKnown() {
    return known;
  }

  public boolean inFileSchema() {
    return inFileSchema;
  }

  public boolean inRequestedSchema() {
    return inRequestedSchema;
  }

  public boolean inFilterSchema() {
    return inFilterSchema;
  }

  @Override
  public String toString() {
    return "PredefinedColumnIOSet{" +
           "known=" + known +
           ", inFileSchema=" + inFileSchema +
           ", inRequestedSchema=" + inRequestedSchema +
           ", inFilterSchema=" + inFilterSchema +
           '}';
  }
}