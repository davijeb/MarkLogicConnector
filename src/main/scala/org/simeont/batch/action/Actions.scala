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
package org.simeont.batch.action

import com.gigaspaces.document.SpaceDocument
import scala.collection.JavaConversions._

/**
 * The super class of all actions to be done against the database.
 */
sealed abstract class Action {
  def update(update: Action): Unit = ()
}

/**
 *  Represent no action to be executed against the database.
 */
case class NoneAction() extends Action

/**
 * A delete for this id is to be executed.
 */
case class DeleteAction(id: String) extends Action

/**
 * An insert of SpaceDocument in the database to be executed.
 */
case class InsertAction(id: String, payload: SpaceDocument) extends Action {
  override def update(action: Action): Unit =
    action match {
      case u: UpdateAction => payload.addProperties(u.payload.getProperties().filter(entry => entry._2 != null))
      case _ => ()
    }

}

/**
 * A partial update of SpaceDocument stored into the database to be executed.
 */
case class UpdateAction(id: String, payload: SpaceDocument) extends Action {
  override def update(action: Action): Unit =
    action match {
      case u: UpdateAction => payload.addProperties(u.payload.getProperties().filter(entry => entry._2 != null))
      case _ => ()
    }
}
