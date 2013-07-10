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
package org.simeont.marklogicconnector.batch.tests

import scala.collection.mutable.{ Map => MMap }
import org.scalatest.FunSuite
import org.simeont.marklogicconnector.batch.OperatinoActionProcessor
import org.simeont.marklogicconnector.batch.action._
import com.gigaspaces.document.SpaceDocument

class OperationActionProcessorTest extends FunSuite {

  val COMMON_KEY = "/check"
  val doc = new SpaceDocument("doc")

  val firstProperty = "fP"
  val secondProperty = "sP"
  val thirdProperty = "tP"

  doc.setProperty(firstProperty, 1)
  doc.setProperty(secondProperty, 1)
  val delete = DeleteAction(COMMON_KEY)
  val insert = InsertAction(COMMON_KEY, doc)
  val updatedInsert = InsertAction(COMMON_KEY, doc)

  doc.setProperty(firstProperty, 2)
  doc.setProperty(secondProperty, null)
  val update = UpdateAction(COMMON_KEY, doc)
  doc.setProperty(thirdProperty, 3)
  val update2 = UpdateAction(COMMON_KEY, doc)
  updatedInsert.update(update)

  test("should update insert action") {
    val COMMON_KEY = "/check"
    val doc = new SpaceDocument("doc")
    doc.setProperty(firstProperty, 1)
    doc.setProperty(secondProperty, 1)

    val updatedInsert = InsertAction(COMMON_KEY, doc)
    updatedInsert.update(update)
    assert(updatedInsert.payload.getProperty(secondProperty) != null)
  }

  test("should add insert action to map") {
    val index: MMap[String, Action] = MMap[String, Action]()

    OperatinoActionProcessor.add(insert, index)
    assert(index.isEmpty === false)
  }

  test("should add delete action to map") {
    val index: MMap[String, Action] = MMap[String, Action]()

    OperatinoActionProcessor.add(delete, index)
    assert(index.isEmpty === false)
  }

  test("should override action on superseding action") {
    val index: MMap[String, Action] = MMap[String, Action]()

    OperatinoActionProcessor.add(update, index)
    assert(index.isEmpty === false)
  }

  test("should update any action stored in the map") {
    val index: MMap[String, Action] = MMap[String, Action]()

    OperatinoActionProcessor.add(delete, index)
    assert(index.get(COMMON_KEY).get === delete)

    OperatinoActionProcessor.add(update, index)
    assert(index.get(COMMON_KEY).get === update)

    OperatinoActionProcessor.add(insert, index)
    assert(index.get(COMMON_KEY).get === insert)

    OperatinoActionProcessor.add(update, index)
    assert(index.get(COMMON_KEY).get === updatedInsert)

    OperatinoActionProcessor.add(delete, index)
    OperatinoActionProcessor.add(update, index)
    OperatinoActionProcessor.add(update2, index)
    assert(index.get(COMMON_KEY).get === update2)
  }
}
