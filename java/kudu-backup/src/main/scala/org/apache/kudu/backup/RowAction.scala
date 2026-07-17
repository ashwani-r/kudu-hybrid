// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.backup

/**
 * A RowAction is used to represent the action associated with a backed up row.
 *
 * These actions are used to represent change-type information for incremental
 * backups and support behavior like [[org.apache.kudu.client.KuduSession.setIgnoreAllNotFoundRows]]
 * when restoring DELETE operations.
 */
sealed abstract class RowAction(private val value: Byte) extends Serializable {
  def getValue: Byte = value
}

object RowAction {
  case object UPSERT extends RowAction(0)
  case object DELETE extends RowAction(1)

  private val byValue: Map[Byte, RowAction] =
    Map(UPSERT.getValue -> UPSERT, DELETE.getValue -> DELETE)

  def fromValue(value: Byte): RowAction = byValue.getOrElse(value, null)
}
