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
package org.simeont.marklogicconnector.tests

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers
import org.simeont.marklogicconnector.factory.CustomContentFactory
import org.simeont.marklogicconnector.xml.BasicMarshaller
import com.gigaspaces.sync.IntroduceTypeData
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import org.simeont.marklogicconnector.xml.spacedescr.SpaceTypeDescriptorMarshaller
import org.simeont.marklogicconnector.marklogic.XQueryHelper
import org.simeont.marklogicconnector.marklogic.MarkLogicWriter
import org.simeont.marklogicconnector.MarkLogicSynchronizationEndpoint
import com.marklogic.xcc.Content
import com.gigaspaces.sync.AddIndexData
import com.gigaspaces.metadata.index.SpaceIndexFactory
import com.gigaspaces.metadata.index.SpaceIndex
import com.gigaspaces.metadata.index.SpaceIndexType
import com.gigaspaces.document.SpaceDocument
import com.gigaspaces.sync.DataSyncOperationType
import com.gigaspaces.sync.OperationsBatchData
import com.gigaspaces.sync.TransactionData
import org.simeont.marklogicconnector.batch.ProcecessedOperationActionHolder

class MarkLogicSynchronizationEndpointTests extends FunSuite with MockitoSugar {

  val customContentFactory = new CustomContentFactory
  customContentFactory.setXmlMarshaller(new BasicMarshaller)
  customContentFactory.setCollections("test")
  customContentFactory.setNamespace("www.simeont.org")
  customContentFactory.setRole("admin")

  val dir = "/test"

  val typ = "Writer"
  val typeDescriptor = new SpaceTypeDescriptorBuilder(typ)
    .idProperty("id").addFixedProperty("data", classOf[Data]).create

  test("should persist spacetypedescriptor") {

    val mockIntroduceTypeData = mock[IntroduceTypeData]
    when(mockIntroduceTypeData.getTypeDescriptor()).thenReturn(typeDescriptor)

    val typeXML = SpaceTypeDescriptorMarshaller marshallSpaceDesc typeDescriptor
    val uri = XQueryHelper.buildSpaceTypeUri(dir, typeDescriptor.getTypeName())
    val content = customContentFactory.generateContent(uri, typeXML)

    val mlWriter = mock[MarkLogicWriter]

    val writer = new MarkLogicSynchronizationEndpoint(customContentFactory, mlWriter, dir)
    writer.onIntroduceType(mockIntroduceTypeData)
    verify(mlWriter, times(1)).persistSpaceDescriptor(new ContentWrapperWithEquals(content))
  }

  test("should not fail on error during spacetypedescriptor persistence error") {
    val mockIntroduceTypeData = mock[IntroduceTypeData]
    when(mockIntroduceTypeData.getTypeDescriptor()).thenReturn(typeDescriptor)

    val typeXML = SpaceTypeDescriptorMarshaller marshallSpaceDesc typeDescriptor
    val uri = XQueryHelper.buildSpaceTypeUri(dir, typeDescriptor.getTypeName())
    val content = customContentFactory.generateContent(uri, typeXML)

    val mlWriter = mock[MarkLogicWriter]
    when(mlWriter.persistSpaceDescriptor(new ContentWrapperWithEquals(content)))
      .thenThrow(new RuntimeException("passed"))
    val writer = new MarkLogicSynchronizationEndpoint(customContentFactory, mlWriter, dir)

    writer.onIntroduceType(mockIntroduceTypeData)

  }

  test("should update indexs element of spacetypedescriptor") {
    val indexs = Array(SpaceIndexFactory.createPropertyIndex("I1", SpaceIndexType.BASIC),
      SpaceIndexFactory.createPropertyIndex("I2", SpaceIndexType.EXTENDED))
    val mockIndexData = mock[AddIndexData]
    when(mockIndexData.getTypeName()).thenReturn("index")
    when(mockIndexData.getIndexes()).thenReturn(indexs)

    val uri = XQueryHelper.buildSpaceTypeUri(dir, "index")

    val mlWriter = mock[MarkLogicWriter]

    val writer = new MarkLogicSynchronizationEndpoint(customContentFactory, mlWriter, dir)

    writer.onAddIndex(mockIndexData)

    verify(mlWriter, times(1)).addElementToDocument(uri, "/spacedesc/indexes",
      SpaceTypeDescriptorMarshaller indexToXml (indexs(0)))
    verify(mlWriter, times(1)).addElementToDocument(uri, "/spacedesc/indexes",
      SpaceTypeDescriptorMarshaller indexToXml (indexs(1)))
  }

  test("should not fail on error during updating index element of spacetypedescriptor") {
    val indexs = Array(SpaceIndexFactory.createPropertyIndex("I1", SpaceIndexType.BASIC),
      SpaceIndexFactory.createPropertyIndex("I2", SpaceIndexType.EXTENDED))
    val mockIndexData = mock[AddIndexData]
    when(mockIndexData.getTypeName()).thenReturn("index")
    when(mockIndexData.getIndexes()).thenReturn(indexs)

    val uri = XQueryHelper.buildSpaceTypeUri(dir, "index")

    val mlWriter = mock[MarkLogicWriter]

    when(mlWriter.addElementToDocument(Matchers.anyString(), Matchers.anyString(), Matchers.anyString()))
      .thenThrow(new RuntimeException("passed"))

    val writer = new MarkLogicSynchronizationEndpoint(customContentFactory, mlWriter, dir)

    writer.onAddIndex(mockIndexData)
  }

  test("should persiste actions") {

    val doc = new SpaceDocument(typ).setProperty("id", "id")
    val doc2 = new SpaceDocument(typ).setProperty("id", "id2")

    val mockDataSyncO1 = mock[com.gigaspaces.sync.DataSyncOperation]
    val mockDataSyncO2 = mock[com.gigaspaces.sync.DataSyncOperation]
    val mockDataSyncO3 = mock[com.gigaspaces.sync.DataSyncOperation]
    val mockDataSyncO4 = mock[com.gigaspaces.sync.DataSyncOperation]
    val mockDataSyncO5 = mock[com.gigaspaces.sync.DataSyncOperation]
    val mockDataSyncO6 = mock[com.gigaspaces.sync.DataSyncOperation]

    when(mockDataSyncO1.getTypeDescriptor()).thenReturn(typeDescriptor)
    when(mockDataSyncO2.getTypeDescriptor()).thenReturn(typeDescriptor)
    when(mockDataSyncO3.getTypeDescriptor()).thenReturn(typeDescriptor)
    when(mockDataSyncO4.getTypeDescriptor()).thenReturn(typeDescriptor)
    when(mockDataSyncO5.getTypeDescriptor()).thenReturn(typeDescriptor)
    when(mockDataSyncO6.getTypeDescriptor()).thenReturn(typeDescriptor)

    when(mockDataSyncO1.getDataAsDocument()).thenReturn(doc)
    when(mockDataSyncO2.getDataAsDocument()).thenReturn(doc)
    when(mockDataSyncO3.getDataAsDocument()).thenReturn(doc)
    when(mockDataSyncO4.getDataAsDocument()).thenReturn(doc2)
    when(mockDataSyncO5.getDataAsDocument()).thenReturn(doc2)
    when(mockDataSyncO6.getDataAsDocument()).thenReturn(doc2)

    when(mockDataSyncO1.getDataSyncOperationType()).thenReturn(DataSyncOperationType.WRITE)
    when(mockDataSyncO2.getDataSyncOperationType()).thenReturn(DataSyncOperationType.UPDATE)
    when(mockDataSyncO3.getDataSyncOperationType()).thenReturn(DataSyncOperationType.PARTIAL_UPDATE)
    when(mockDataSyncO4.getDataSyncOperationType()).thenReturn(DataSyncOperationType.REMOVE)
    when(mockDataSyncO5.getDataSyncOperationType()).thenReturn(DataSyncOperationType.REMOVE_BY_UID)
    when(mockDataSyncO5.getDataSyncOperationType()).thenReturn(DataSyncOperationType.CHANGE)

    val data = Array(mockDataSyncO1, mockDataSyncO2, mockDataSyncO3, mockDataSyncO4, mockDataSyncO5)

    val processed = ProcecessedOperationActionHolder(Some(Array(new ContentWrapperWithEquals(
      customContentFactory.generateContent(XQueryHelper.buildDataUri(dir, typ, "id"), doc)))),
      Some(List(XQueryHelper.buildDataUri(dir, typ, "id2"))), None)

    val operationsBatchData = mock[OperationsBatchData]
    val transactionData = mock[TransactionData]

    when(operationsBatchData.getBatchDataItems()).thenReturn(data)
    when(transactionData.getTransactionParticipantDataItems()).thenReturn(data)

    val mlWriter = mock[MarkLogicWriter]

    val writer = new MarkLogicSynchronizationEndpoint(customContentFactory, mlWriter, dir)

    writer.onOperationsBatchSynchronization(operationsBatchData)
    verify(mlWriter, times(1)).persistAll(new ProcessedOperationWrapperWithEquals(processed))

    writer.onTransactionSynchronization(transactionData)
    verify(mlWriter, times(2)).persistAll(new ProcessedOperationWrapperWithEquals(processed))
  }

  test("should not crash on error during data persistence") {
    val doc = new SpaceDocument(typ).setProperty("id", "id")
    val mockDataSyncO1 = mock[com.gigaspaces.sync.DataSyncOperation]
    when(mockDataSyncO1.getTypeDescriptor()).thenReturn(typeDescriptor)
    when(mockDataSyncO1.getDataAsDocument()).thenReturn(doc)
    when(mockDataSyncO1.getDataSyncOperationType()).thenReturn(DataSyncOperationType.WRITE)
    val data = Array(mockDataSyncO1)

    val operationsBatchData = mock[OperationsBatchData]
    val transactionData = mock[TransactionData]

    when(operationsBatchData.getBatchDataItems()).thenReturn(data)
    when(transactionData.getTransactionParticipantDataItems()).thenReturn(data)

    val mlWriter = mock[MarkLogicWriter]
    when(mlWriter.persistAll(Matchers.any(classOf[ProcecessedOperationActionHolder])))
      .thenThrow(new RuntimeException("Testing"))
    val writer = new MarkLogicSynchronizationEndpoint(customContentFactory, mlWriter, dir)

    writer.onOperationsBatchSynchronization(operationsBatchData)
    writer.onTransactionSynchronization(transactionData)
  }
}

class ContentWrapperWithEquals(content: Content) extends Content {
  override def equals(obj: Any): Boolean = {
    obj match {
      case con: Content => {
        content.getUri() == con.getUri() &&
          content.isRewindable() == con.isRewindable() &&
          content.size() == con.size()
      }
      case _ => false
    }
  }
  override def hashCode: Int = content.hashCode()

  override def close(): Unit = content.close
  override def getCreateOptions(): com.marklogic.xcc.ContentCreateOptions = content.getCreateOptions()
  override def getUri(): String = content.getUri()
  override def isRewindable(): Boolean = content.isRewindable()
  override def openDataStream(): java.io.InputStream = content.openDataStream()
  override def rewind(): Unit = content.rewind()
  override def size(): Long = content.size()
}

class ProcessedOperationWrapperWithEquals(
  poah: ProcecessedOperationActionHolder) extends ProcecessedOperationActionHolder(
  poah.contents, poah.deleteIds, poah.updates) {
  //Array equals does not work
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ProcecessedOperationActionHolder => {
        ((that.contents.isEmpty && contents.isEmpty) || (
          that.contents.isDefined && contents.isDefined && contents.get.toList == that.contents.get.toList)) &&
          that.deleteIds == deleteIds && that.updates == updates
      }
      case _ => false
    }
  }

  override def hashCode: Int = poah.hashCode
}