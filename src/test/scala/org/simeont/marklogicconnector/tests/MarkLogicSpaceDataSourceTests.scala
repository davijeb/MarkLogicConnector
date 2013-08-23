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
import com.gigaspaces.metadata.SpaceTypeDescriptor
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.marklogic.xcc.ResultSequence
import org.simeont.marklogicconnector.xml.BasicMarshaller
import org.simeont.marklogicconnector.xml.spacedescr.SpaceTypeDescriptorMarshaller
import org.simeont.marklogicconnector.marklogic.MarkLogicReader
import org.simeont.marklogicconnector.marklogic.XQueryHelper
import org.simeont.marklogicconnector.MarkLogicSpaceDataSource
import com.marklogic.xcc.exceptions.RequestException
import com.marklogic.xcc.Request
import com.gigaspaces.document.SpaceDocument
import com.marklogic.xcc.ResultItem
import com.gigaspaces.datasource.DataSourceIdQuery
import com.gigaspaces.datasource.DataSourceIdsQuery
import org.simeont.marklogicconnector.iterators.ObjectMLIterator
import com.gigaspaces.datasource.DataSourceQuery
import org.simeont.marklogicconnector.sql.parser.XmlToXPathDecoder
import scala.xml.XML
import org.simeont.marklogicconnector.sql.parser.ComparisonType
import com.gigaspaces.internal.document.DocumentObjectConverterInternal
import com.gigaspaces.datasource.DataSourceSQLQuery
import org.simeont.marklogicconnector.sql.parser.GsSqlParser
import org.simeont.marklogicconnector.sql.parser.GsSqlDecoder

class MarkLogicSpaceDataSourceTests extends FunSuite with MockitoSugar {

  val marshaller = new BasicMarshaller
  val dir = "/test"
  val namespace = "www.simeont.org"

  val typ = "Query"
  val typeDescriptor = new SpaceTypeDescriptorBuilder(typ)
    .idProperty("id").addFixedProperty("data", classOf[Data]).create

  test("should return SpaceTypeDescriptors ") {

    val spaceDescriptor = new SpaceTypeDescriptorBuilder(classOf[Data], null).create
    val spaceDescriptor2 = new SpaceTypeDescriptorBuilder("Test").idProperty("test").create
    val marshalledSpaceDescriptor = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spaceDescriptor)
    val marshalledSpaceDescriptor2 = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spaceDescriptor2)
    val testdir = XQueryHelper.buildSpaceTypeDir(dir)
    val query = XQueryHelper.buildDirectoryQuerigXQuery(testdir, "1")
    val mockReader = mock[MarkLogicReader]
    val mockResult = mock[ResultSequence]

    when(mockReader.readSpaceTypeDescriptors(query)).thenReturn(mockResult)
    when(mockResult.asStrings()).thenReturn(Array(marshalledSpaceDescriptor, marshalledSpaceDescriptor2))
    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)

    val iterator = source.initialMetadataLoad()
    val first = iterator.next
    val last = iterator.next

    assert(iterator.hasNext() == false)
    if (first.getTypeName() == "Test") {
      compareSpaceTypes(first, spaceDescriptor2)
      compareSpaceTypes(last, spaceDescriptor)
    } else {
      compareSpaceTypes(last, spaceDescriptor2)
      compareSpaceTypes(first, spaceDescriptor)
    }
  }

  test("should crash if error occure in initial metadata load") {
    val mockReader = mock[MarkLogicReader]
    val testdir = XQueryHelper.buildSpaceTypeDir(dir)
    val query = XQueryHelper.buildDirectoryQuerigXQuery(testdir, "1")

    when(mockReader.readSpaceTypeDescriptors(query)).thenThrow(new RuntimeException("success"))
    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)

    try {
      source.initialMetadataLoad()
      assert(false)
    } catch { case _: Throwable => assert(true) }

  }

  test("should return data on initial dataload") {
    val testdir = XQueryHelper.buildDataDir(dir)
    val query = XQueryHelper.buildDirectoryQuerigXQuery(testdir, "infinity")
    val doc = new SpaceDocument("Test")
    val dataString = marshaller.toXML(doc)

    val mockReader = mock[MarkLogicReader]
    val mockResult = mock[ResultSequence]
    val mockItem = mock[ResultItem]

    when(mockReader.readMany(query)).thenReturn(mockResult)
    when(mockResult.hasNext()).thenReturn(true, false)
    when(mockResult.next()).thenReturn(mockItem)
    when(mockItem.isFetchable()).thenReturn(true)
    when(mockItem.asString()).thenReturn(dataString)

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    val iterator = source.initialDataLoad()
    assert(iterator.next === doc)
    assert(iterator.next === null)

  }

  test("should return null if error occure in initial data load") {
    val mockReader = mock[MarkLogicReader]
    val testdir = XQueryHelper.buildDataDir(dir)
    val query = XQueryHelper.buildDirectoryQuerigXQuery(testdir, "infinity")

    when(mockReader.readMany(query)).thenThrow(new RuntimeException("success"))
    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)

    try {
      source.initialDataLoad()
      assert(false)
    } catch { case _: Throwable => assert(true) }
  }

  test("should read by single id") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceIdQuery = mock[DataSourceIdQuery]

    when(mockDataSourceIdQuery.getTypeDescriptor).thenReturn(typeDescriptor)
    when(mockDataSourceIdQuery.getId).thenReturn("key", "key1")

    val uri = XQueryHelper.buildDataUri(dir, typ, "key")
    val query = XQueryHelper.builDocumentQueringXQuery(namespace, uri, "", "")

    val data = new SpaceDocument(typ)
    data.setProperty("id", "key")
    data.setProperty("data", Data())

    val marshalled = marshaller.toXML(data)
    when(mockReader.read(query)).thenReturn(marshalled)

    val uri1 = XQueryHelper.buildDataUri(dir, typ, "key1")
    val query1 = XQueryHelper.builDocumentQueringXQuery(namespace, uri1, "", "")

    when(mockReader.read(query1)).thenReturn(null)

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    assert(source.getById(mockDataSourceIdQuery) === data)
    assert(source.getById(mockDataSourceIdQuery) === null)
  }

  test("should return null on error in read by id") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceIdQuery = mock[DataSourceIdQuery]

    when(mockDataSourceIdQuery.getTypeDescriptor).thenReturn(typeDescriptor)
    when(mockDataSourceIdQuery.getId).thenReturn("key", "key1")

    val uri = XQueryHelper.buildDataUri(dir, typ, "key")
    val query = XQueryHelper.builDocumentQueringXQuery(namespace, uri, "", "")

    when(mockReader.read(query)).thenThrow(new RuntimeException("ReadById error"))

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    assert(source.getById(mockDataSourceIdQuery) === null)
  }

  test("should read by multiple ids") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceIdsQuery = mock[DataSourceIdsQuery]

    when(mockDataSourceIdsQuery.getTypeDescriptor).thenReturn(typeDescriptor)
    when(mockDataSourceIdsQuery.getIds).thenReturn(Array("key", "key1"), Array("key", "key1"))

    val uris = Array("key", "key1").map(id =>
      "\"" + XQueryHelper.buildDataUri(dir, typ, id.toString) + "\"").mkString(", ")
    val query = XQueryHelper.builDocumentQueringXQuery("", "(" + uris + ")", "", "")

    val data = new SpaceDocument(typ)
    data.setProperty("id", "key")
    data.setProperty("data", Data())
    val marshalled = marshaller.toXML(data)

    val mockResultSequence = mock[ResultSequence]
    val mockResultItem = mock[ResultItem]

    when(mockResultItem.isFetchable()).thenReturn(true)
    when(mockResultItem.asString()).thenReturn(marshalled)
    when(mockResultSequence.hasNext()).thenReturn(true, false)
    when(mockResultSequence.next).thenReturn(mockResultItem)
    when(mockReader.readMany(query)).thenReturn(mockResultSequence)

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    val iterator = source.getDataIteratorByIds(mockDataSourceIdsQuery)
    assert(iterator.next === data)
    assert(iterator.next === null)
  }

  test("should return null on error in read by ids") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceIdQuery = mock[DataSourceIdsQuery]

    when(mockDataSourceIdQuery.getTypeDescriptor).thenReturn(typeDescriptor)
    when(mockDataSourceIdQuery.getIds).thenReturn(Array("key", "key1"), Array("key", "key1"))

    val uris = Array("key", "key1").map(id =>
      "\"" + XQueryHelper.buildDataUri(dir, typ, id.toString) + "\"").mkString(", ")
    val query = XQueryHelper.builDocumentQueringXQuery("", "(" + uris + ")", "", "")

    val data = new SpaceDocument(typ)
    data.setProperty("id", "key")
    data.setProperty("data", Data())
    val marshalled = marshaller.toXML(data)

    when(mockReader.readMany(query)).thenThrow(new RuntimeException("ReadByIds error"))

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    assert(source.getDataIteratorByIds(mockDataSourceIdQuery).next === null)

  }

  test("should return data base on spacedocument template") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceIdQuery = mock[DataSourceQuery]

    when(mockDataSourceIdQuery.supportsAsSQLQuery()).thenReturn(false)
    when(mockDataSourceIdQuery.supportsTemplateAsObject()).thenReturn(false)
    when(mockDataSourceIdQuery.supportsTemplateAsDocument()).thenReturn(true)

    val data = new SpaceDocument(typ)
    data.setProperty("id", "key")
    data.setProperty("data", Data())
    val marshalled = marshaller.toXML(data)

    when(mockDataSourceIdQuery.getTemplateAsDocument()).thenReturn(data)

    val mockResultSequence = mock[ResultSequence]
    val mockResultItem = mock[ResultItem]
    when(mockResultItem.isFetchable()).thenReturn(true)
    when(mockResultItem.asString()).thenReturn(marshalled)
    when(mockResultSequence.hasNext()).thenReturn(true, false)
    when(mockResultSequence.next).thenReturn(mockResultItem)

    val node = XML.loadString(marshalled)
    val xpath = XmlToXPathDecoder.extractXPath(node, ComparisonType.Equal)
    val indexFriendlyXpath = xpath.map(currentPath => currentPath.xpath.replaceFirst(" " + node.label, " ."))
    val query = XQueryHelper.builDocumentQueringXQuery(namespace, "", node.label, indexFriendlyXpath.mkString(" and "))
    when(mockReader.readMany(query)).thenReturn(mockResultSequence)

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    val iterator = source.getDataIterator(mockDataSourceIdQuery)
    assert(iterator.next === data)
    assert(iterator.next === null)
  }

  test("should return data base on class template") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceIdQuery = mock[DataSourceQuery]

    when(mockDataSourceIdQuery.supportsAsSQLQuery()).thenReturn(false)
    when(mockDataSourceIdQuery.supportsTemplateAsObject()).thenReturn(true)
    when(mockDataSourceIdQuery.supportsTemplateAsDocument()).thenReturn(false)

    val data = Data()
    val asDoc = DocumentObjectConverterInternal.instance().toSpaceDocument(data)
    val marshalled = marshaller.toXML(asDoc)

    when(mockDataSourceIdQuery.getTemplateAsObject()).thenReturn(data, data)

    val mockResultSequence = mock[ResultSequence]
    val mockResultItem = mock[ResultItem]
    when(mockResultItem.isFetchable()).thenReturn(true)
    when(mockResultItem.asString()).thenReturn(marshalled)
    when(mockResultSequence.hasNext()).thenReturn(true, false)
    when(mockResultSequence.next).thenReturn(mockResultItem)

    val node = XML.loadString(marshalled)
    val xpath = XmlToXPathDecoder.extractXPath(node, ComparisonType.Equal)
    val indexFriendlyXpath = xpath.map(currentPath => currentPath.xpath.replaceFirst(" " + node.label, " ."))
    val query = XQueryHelper.builDocumentQueringXQuery(namespace, "", node.label, indexFriendlyXpath.mkString(" and "))
    when(mockReader.readMany(query)).thenReturn(mockResultSequence)

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    val iterator = source.getDataIterator(mockDataSourceIdQuery)
    assert(iterator.next === asDoc)
    assert(iterator.next === null)
  }

  test("should return data base on sql template") {
    val mockReader = mock[MarkLogicReader]
    val mockDataSourceQuery = mock[DataSourceQuery]

    when(mockDataSourceQuery.getTypeDescriptor).thenReturn(typeDescriptor)
    when(mockDataSourceQuery.supportsAsSQLQuery()).thenReturn(true)
    when(mockDataSourceQuery.supportsTemplateAsObject()).thenReturn(false)
    when(mockDataSourceQuery.supportsTemplateAsDocument()).thenReturn(false)

    val mockDataSourceSQLQuery = mock[DataSourceSQLQuery]
    when(mockDataSourceSQLQuery.getQuery()).thenReturn("id = ?")
    when(mockDataSourceSQLQuery.getQueryParameters()).thenReturn(Array("1"), Array("1"))
    when(mockDataSourceQuery.getAsSQLQuery()).thenReturn(mockDataSourceSQLQuery)

    val data = new SpaceDocument(typ)
    data.setProperty("id", "key")
    data.setProperty("data", Data())
    val marshalled = marshaller.toXML(data)

    val mockResultSequence = mock[ResultSequence]
    val mockResultItem = mock[ResultItem]
    when(mockResultItem.isFetchable()).thenReturn(true)
    when(mockResultItem.asString()).thenReturn(marshalled)
    when(mockResultSequence.hasNext()).thenReturn(true, false)
    when(mockResultSequence.next).thenReturn(mockResultItem)

    val toDecodeSQL = GsSqlParser("id = ?")
    val queryParameters = List("1")
    val decodedSQL = new GsSqlDecoder(marshaller).decodeSQL(toDecodeSQL, queryParameters)
    val queryXPath =
      XQueryHelper.builDocumentQueringXQuery(namespace, "", typeDescriptor.getTypeName(), decodedSQL)
    when(mockReader.readMany(queryXPath)).thenReturn(mockResultSequence)

    val source = new MarkLogicSpaceDataSource(marshaller, mockReader, dir, namespace)
    val iterator = source.getDataIterator(mockDataSourceQuery)
    assert(iterator.next === data)
    assert(iterator.next === null)

  }

  private[this] def compareSpaceTypes(t1: SpaceTypeDescriptor, t2: SpaceTypeDescriptor): Unit = {
    assert(t1.getDocumentWrapperClass() === t2.getDocumentWrapperClass())
    assert(t1.getFifoGroupingIndexesPaths() === t2.getFifoGroupingIndexesPaths())
    assert(t1.getFifoGroupingPropertyPath() === t2.getFifoGroupingPropertyPath())
    assert(t1.getFifoSupport() === t2.getFifoSupport)
    assert(t1.getIdPropertyName() === t2.getIdPropertyName)
    assert(t1.getIndexes() === t2.getIndexes)
    assert(t1.getNumOfFixedProperties() === t2.getNumOfFixedProperties)
    assert(t1.getObjectClass() === t2.getObjectClass)
    assert(t1.getRoutingPropertyName() == t2.getRoutingPropertyName)
    assert(t1.getStorageType() === t2.getStorageType)
    assert(t1.getSuperTypeName() === t2.getSuperTypeName)
    assert(t1.getTypeName() === t2.getTypeName)
    assert(t1.getTypeSimpleName() === t2.getTypeSimpleName)
    assert(t1.isAutoGenerateId() === t2.isAutoGenerateId)
    assert(t1.isConcreteType() === t2.isConcreteType)
    assert(t1.isReplicable() === t2.isReplicable)
    assert(t1.supportsDynamicProperties() === t2.supportsDynamicProperties)
    assert(t1.supportsOptimisticLocking() === t2.supportsOptimisticLocking)
  }
}

case class Data()