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
import com.marklogic.xcc.ContentSource
import com.marklogic.xcc.Session
import com.marklogic.xcc.Content
import com.marklogic.xcc.exceptions.RequestException
import com.marklogic.xcc.Request
import org.simeont.marklogicconnector.marklogic.MarkLogicWriter
import org.simeont.marklogicconnector.batch.ProcecessedOperationActionHolder

class MarkLogicWriterTests extends FunSuite with MockitoSugar {

  val namespace = "www.simeont.org"

  test("should not crash if exception occure during insertion of spacetypedescriptor") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    val mockContent = mock[Content]
    when(mockSession.insertContent(mockContent)).thenThrow(new RequestException("test", null))

    val markLogicWriter = new MarkLogicWriter(mockContentSource, namespace)
    try {
      markLogicWriter.persistSpaceDescriptor(mockContent)
      assert(true)
    } catch { case _: Throwable => assert(false) }
  }

  test("should not crash if exception occure during update of spacetypedescriptor") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.submitRequest(Matchers.any(classOf[Request]))).thenThrow(new RequestException("test", null))

    val markLogicWriter = new MarkLogicWriter(mockContentSource, namespace)
    try {
      markLogicWriter.addElementToDocument("test.xml", "/test", "<newElement/>")
      assert(true)
    } catch { case _: Throwable => assert(false) }
  }

  test("should not connect to MarkLogic if there is no data changes") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.submitRequest(Matchers.any(classOf[Request]))).thenThrow(new RequestException("test", null))
    when(mockSession.insertContent(Matchers.any(classOf[Array[Content]]))).thenThrow(new RequestException("test", null))

    val markLogicWriter = new MarkLogicWriter(mockContentSource, namespace)
    val data = ProcecessedOperationActionHolder(None, None, None)
    try {
      markLogicWriter.persistAll(data)

      assert(true)
    } catch { case _: Throwable => assert(false) }
  }

  test("should throw exception if error occure during data update") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]
    val content = mock[Content]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.submitRequest(Matchers.any(classOf[Request]))).thenThrow(new RequestException("test", null))
    when(mockSession.insertContent(Array(content))).thenThrow(new RequestException("success", null))

    val markLogicWriter = new MarkLogicWriter(mockContentSource, namespace)
    val data = ProcecessedOperationActionHolder(Some(Array(content)), None, None)
    try {
      markLogicWriter.persistAll(data)

      assert(false)
    } catch {
      case ex: Throwable => {
        assert(ex.getMessage() === "success")
        verify(mockSession).rollback()
      }
    }
  }

  test("should send request to delete data") {
    val mockContentSource = mock[ContentSource]
    val mockSession = mock[Session]
    val content = mock[Content]

    when(mockContentSource.newSession()).thenReturn(mockSession)
    when(mockSession.submitRequest(Matchers.any(classOf[Request]))).thenThrow(new RequestException("success", null))

    val markLogicWriter = new MarkLogicWriter(mockContentSource, namespace)
    val data = ProcecessedOperationActionHolder(Some(Array(content)), Some(List("array")), None)
    try {
      markLogicWriter.persistAll(data)

      assert(false)
    } catch {
      case ex: Throwable => {
        assert(ex.getMessage() === "success")
        verify(mockSession).rollback()
      }
    }
  }
}
