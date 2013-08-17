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
package org.simeont.marklogicconnector.marklogic.tests

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers
import org.simeont.marklogicconnector.marklogic.MarkLogicReader
import com.marklogic.xcc.ContentSource
import com.marklogic.xcc.Session
import com.marklogic.xcc.Request
import com.marklogic.xcc.exceptions.RequestException
import com.marklogic.xcc.AdhocQuery
import com.marklogic.xcc.ResultSequence
import com.marklogic.xcc.ResultItem

class MarkLogicReaderTests extends FunSuite with MockitoSugar {
  val namespace = "www.simeont.org"

  test("should crash while reading if error occure") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]
    val mockAdHocQuery = mock[AdhocQuery]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.newAdhocQuery("xdmp:directory(\"/\"")).thenReturn(mockAdHocQuery)
    when(mockSession.submitRequest(mockAdHocQuery)).thenThrow(new RequestException("success", null))
    val markLogicReader = new MarkLogicReader(mockContentSource, namespace)

    try {
      markLogicReader.readSpaceTypeDescriptors("xdmp:directory(\"/\"")
      assert(false)
    } catch {
      case ex: Throwable => {
        assert(ex.isInstanceOf[RequestException])
        assert(ex.getMessage === "success")
      }
    }

    try {
      markLogicReader.readMany("xdmp:directory(\"/\"")
      assert(false)
    } catch {
      case ex: Throwable => {
        assert(ex.isInstanceOf[RequestException])
        assert(ex.getMessage === "success")
      }
    }
    try {
      markLogicReader.read("xdmp:directory(\"/\"")
      assert(false)
    } catch {
      case ex: Throwable => {
        assert(ex.isInstanceOf[RequestException])
        assert(ex.getMessage === "success")
      }
    }
  }

  test("should return null if read method has not found anything") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]
    val mockAdHocQuery = mock[AdhocQuery]
    val mockResultSequence = mock[ResultSequence]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.newAdhocQuery("xdmp:doc(\"doc\"")).thenReturn(mockAdHocQuery)
    when(mockSession.submitRequest(mockAdHocQuery)).thenReturn(mockResultSequence)
    val markLogicReader = new MarkLogicReader(mockContentSource, namespace)

    assert(markLogicReader.read("xdmp:doc(\"doc\"") === null)
  }
  
  test("should return object on single read") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]
    val mockAdHocQuery = mock[AdhocQuery]
    val mockResultSequence = mock[ResultSequence]
    val mockResultItem = mock[ResultItem]
    
    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.newAdhocQuery("xdmp:doc(\"doc\"")).thenReturn(mockAdHocQuery)
    when(mockSession.submitRequest(mockAdHocQuery)).thenReturn(mockResultSequence)
    when(mockResultSequence.hasNext()).thenReturn(true,false)
    when(mockResultSequence.next).thenReturn(mockResultItem)
    when(mockResultItem.asString()).thenReturn("test")
    
    val markLogicReader = new MarkLogicReader(mockContentSource, namespace)

    assert(markLogicReader.read("xdmp:doc(\"doc\"") === "test")
  }
}
