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
package org.simeont.marklogicconnector

import scala.xml.XML
import java.util.logging.Logger
import com.gigaspaces.datasource.DataIterator
import com.gigaspaces.datasource.DataSourceIdQuery
import com.gigaspaces.datasource.DataSourceIdsQuery
import com.gigaspaces.datasource.DataSourceQuery
import com.gigaspaces.datasource.SpaceDataSource
import com.gigaspaces.metadata.SpaceTypeDescriptor
import com.gigaspaces.internal.document.DocumentObjectConverterInternal
import org.simeont.marklogicconnector.marklogic.XQueryHelper
import org.simeont.marklogicconnector.sql.parser._
import org.simeont.marklogicconnector.sql.parser.ComparisonType._
import org.simeont.marklogicconnector.xml.Marshaller
import org.simeont.marklogicconnector.iterators._
import java.util.logging.Level

class MarkLogicSpaceDataSource(marshaller: Marshaller, reader: ReaderInterface,
  dirPath: String, namespace: String) extends SpaceDataSource {

  private[this] val logger: Logger = Logger.getLogger(classOf[MarkLogicSpaceDataSource].getCanonicalName())
  private[this] val privateConverter = DocumentObjectConverterInternal.instance() //Gs Internal access
  private[this] val sqlDecoder = new GsSqlDecoder(marshaller)

  override def getById(idQuery: DataSourceIdQuery): Object = {
    val typ = idQuery.getTypeDescriptor().getTypeName()
    val id = idQuery.getId().toString
    val uri = XQueryHelper.buildDataUri(dirPath, typ, id)
    val query = XQueryHelper.builDocumentQueringXQuery(namespace, uri, "", "")
    logger.finest(query)
    try {
      val data = reader.read(query)
      if (data == null) data
      else marshaller.fromXML(data)
    } catch { case ex: Throwable => { logError(ex); null } }
  }

  override def getDataIterator(query: DataSourceQuery): DataIterator[Object] = {

    if (query.supportsTemplateAsDocument()) {
      val doc = marshaller.toXML(query.getTemplateAsDocument())
      queryBasedOnXml(doc)

    } else if (query.supportsTemplateAsObject()) {
      val doc = privateConverter.toSpaceDocument(query.getTemplateAsObject())
      val docMarshalled = marshaller.toXML(doc)
      queryBasedOnXml(docMarshalled)

    } else if (query.supportsAsSQLQuery()) {
      val sql = query.getAsSQLQuery()
      val toDecodeSQL = GsSqlParser(sql.getQuery())
      val queryParameters = if (sql.getQueryParameters() != null) sql.getQueryParameters().toList else List()
      val decodedSQL = sqlDecoder.decodeSQL(toDecodeSQL, queryParameters)
      val queryXPath =
        XQueryHelper.builDocumentQueringXQuery(namespace, "", query.getTypeDescriptor().getTypeName(), decodedSQL)
      logger.finest(queryXPath)
      errorSafetyManyDataIteratorConstruction(queryXPath)
    } else new ObjectMLIterator;
  }

  private[this] def queryBasedOnXml(doc: String): DataIterator[Object] = {
    val node = XML.loadString(doc)
    val xpath = XmlToXPathDecoder.extractXPath(node, ComparisonType.Equal)
    val indexFriendlyXpath = xpath.map(currentPath => currentPath.xpath.replaceFirst(" " + node.label, " ."))

    val query = XQueryHelper.builDocumentQueringXQuery(namespace, "", node.label, indexFriendlyXpath.mkString(" and "))
    logger.finest(query)
    errorSafetyManyDataIteratorConstruction(query)
  }

  override def getDataIteratorByIds(idsQuery: DataSourceIdsQuery): DataIterator[Object] = {

    val typ = idsQuery.getTypeDescriptor().getTypeName()
    val uris = idsQuery.getIds().map(id =>
      "\"" + XQueryHelper.buildDataUri(dirPath, typ, id.toString) + "\"").mkString(", ")
    val query = XQueryHelper.builDocumentQueringXQuery("", "(" + uris + ")", "", "")
    logger.finest(query)
    errorSafetyManyDataIteratorConstruction(query)
  }

  override def initialDataLoad(): DataIterator[Object] = {
    logger.info("InitialDataLoad called.")
    val dir = XQueryHelper.buildDataDir(dirPath)
    val query = XQueryHelper.buildDirectoryQuerigXQuery(dir, "infinity")
    new ObjectMLIterator(reader.readMany(query), marshaller)
  }

  override def initialMetadataLoad(): DataIterator[SpaceTypeDescriptor] = {
    logger.info("InitialMetadataLoad called.")
    val dir = XQueryHelper.buildSpaceTypeDir(dirPath)
    val query = XQueryHelper.buildDirectoryQuerigXQuery(dir, "1")

    new SpaceDescriptorMLIterator(reader.readSpaceTypeDescriptors(query))

  }

  override def supportsInheritance(): Boolean = false

  private[this] def errorSafetyManyDataIteratorConstruction(query: String): DataIterator[Object] = {
    try {
      new ObjectMLIterator(reader.readMany(query), marshaller)
    } catch {
      case ex: Throwable => {
        logError(ex)
        new ObjectMLIterator()
      }
    }
  }

  private[this] def logError(ex: Throwable): Unit = {
    val msg = "Cannot execute query due to " + ex.getMessage() + "\n" + ex.getMessage()
    logger.log(Level.SEVERE, msg)
  }
}
