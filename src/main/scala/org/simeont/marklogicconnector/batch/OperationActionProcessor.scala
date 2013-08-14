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
package org.simeont.marklogicconnector.batch

import scala.collection.mutable.{ Map => MMap }
import com.marklogic.xcc.Content
import org.simeont.marklogicconnector.factory.CustomContentFactory
import org.simeont.marklogicconnector.batch.action._

/**
 * Object that holds helper methods for Actions processing
 *
 * @author Tomo Simeonov
 */
object OperatinoActionProcessor {

  def add(update: UpdateAction, index: MMap[String, Action]): Unit = {
    index.getOrElse(update.id, None) match {
      case insert: InsertAction => insert.update(update)
      case update: UpdateAction => update.update(update)
      case _ => index.put(update.id, update)
    }
  }

  def add(delete: DeleteAction, index: MMap[String, Action]): Unit = {
    index.put(delete.id, delete)
  }

  def add(insert: InsertAction, index: MMap[String, Action]): Unit = {
    index.put(insert.id, insert)
  }

  /**
   * Transforms an action map to a the data holder object to be used by the writer interface
   *
   * @param actionMap The action map produced from the batch data
   *
   * @param customContentFactory The factory to use to translate insert action into MarkLogic content
   */
  def transform(actionMap: MMap[String, Action],
    customContentFactory: CustomContentFactory): ProcecessedOperationActionHolder = {

    var contents = Array[Content]()
    var deleteIds = List[String]()
    var updates = List[UpdateAction]()

    actionMap.values.foreach(action => action match {
      case i: InsertAction => contents = contents.+:(customContentFactory.generateContent(i.id, i.payload))
      case u: UpdateAction => updates = updates.::(u)
      case d: DeleteAction => deleteIds = deleteIds.::(d.id)
      case _ => ()
    })

    val cont = if(contents.isEmpty) None else Some(contents)
    val delIds = if(deleteIds.isEmpty) None else Some(deleteIds)
    ProcecessedOperationActionHolder(cont, delIds, None)
  }
}
