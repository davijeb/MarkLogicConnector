/*
 * Copyright 2013 Tomo Simeonov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.simeont.batch

import com.marklogic.xcc.Content
import org.simeont.batch.action.UpdateAction

/**
 * A holder object to provide information for the writer interface. Contains data to be inserted, data to be removed
 * and data to be partially updated.
 */
case class ProcecessedOperationActionHolder(contents: Option[Array[Content]], deleteIds: Option[List[String]],
  updates: Option[List[UpdateAction]]) {

  private[this] val version = "xquery version \"1.0-ml\";"

  /**
   *   Get the XQuery code for deleting
   */
  def getDeleteXqueryCode(nameSpace: String) = deleteIds match {
    case Some(delIds) => version + "declare namespace namespace = \"" + nameSpace + "\";" +
    "let $del := ('" + delIds.mkString("','") + "')" + " return xdmp:document-delete($del)"
    case None => ""
  }

  /**
   * Get the XQuery code for updating
   */
  def getUpdateXqueryCode(nameSpace: String) = ""

  /**
   * Check if an updated should be performed
   */
  def doUpdate = false

  /**
   * Check if a delete should be performed
   */
  def doDelete = deleteIds match {
    case Some(delIds) if !delIds.isEmpty => true
    case _ => false
  }
}
