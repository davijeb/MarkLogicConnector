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
package org.simeont.marklogicconnector.iterators.tests

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.marklogic.xcc.ResultSequence
import com.gigaspaces.document.SpaceDocument
import org.simeont.marklogicconnector.xml.BasicMarshaller
import com.marklogic.xcc.ResultItem
import org.simeont.marklogicconnector.iterators.ObjectMLIterator
import scala.util.control.Exception
import com.marklogic.xcc.impl.StreamingResultSequence

class ObjectMLIteratorTests extends MockitoSugar with FunSuite {

  val marshaller = new BasicMarshaller
  val data = new SpaceDocument("Test")
  val dataMarshalled = marshaller.toXML(data)

  val resultItem = mock[ResultItem]
  when(resultItem.isFetchable()).thenReturn(false, false, true)
  when(resultItem.asString()).thenReturn(dataMarshalled)

  test("should iterate on no data") {
    val testSpecificResultSequence = mock[ResultSequence]
    when(testSpecificResultSequence.hasNext).thenReturn(false)

    val iterator = new ObjectMLIterator(testSpecificResultSequence, marshaller)
    assert(iterator.hasNext() === false)
    assert(iterator.next === null)
  }

  test("should iterate on one data item") {
    val testSpecificResultSequence = mock[ResultSequence]
    when(testSpecificResultSequence.hasNext).thenReturn(true, false)
    when(testSpecificResultSequence.next()).thenReturn(resultItem)

    val iterator = new ObjectMLIterator(testSpecificResultSequence, marshaller)
    assert(iterator.next === data)
    assert(iterator.next === null)
  }

  test("should iterate on many data items") {
    val testSpecificResultSequence = mock[ResultSequence]
    when(testSpecificResultSequence.hasNext).thenReturn(true, true, false)
    when(testSpecificResultSequence.next()).thenReturn(resultItem)

    val iterator = new ObjectMLIterator(testSpecificResultSequence, marshaller)
    assert(iterator.next === data)
    assert(iterator.next === data)
    assert(iterator.next === null)
  }

  test("should close iteratator on error in next") {
    val testSpecificResultSequence = mock[StreamingResultSequence]
    when(testSpecificResultSequence.hasNext).thenReturn(true, true, false)

    val corruptedRresultItem = mock[ResultItem]
    when(corruptedRresultItem.isFetchable()).thenReturn(false, false, true)
    when(corruptedRresultItem.asString()).thenReturn("<")

    when(testSpecificResultSequence.next()).thenReturn(corruptedRresultItem, resultItem)
    when(testSpecificResultSequence.close()).thenCallRealMethod()
    val iterator = new ObjectMLIterator(testSpecificResultSequence, marshaller)
    try { iterator.next; assert(false) } catch { case x: Throwable => assert(x.isInstanceOf[NullPointerException]) }
  }
}
