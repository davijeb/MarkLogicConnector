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
import org.simeont.marklogicconnector.sql.parser.GsSqlParser
import org.simeont.marklogicconnector.sql.parser.XmlToXPathDecoder
import org.simeont.marklogicconnector.sql.parser.data._
import org.simeont.marklogicconnector.sql.parser.data.ComparisonType._
import org.simeont.marklogicconnector.xml.Marshaller
import org.simeont.marklogicconnector.iterators.SpaceDescriptorMLIterator
import org.simeont.marklogicconnector.iterators.ObjectMLIterator
import org.simeont.marklogicconnector.sql.parser.XPath

class MarkLogicSpaceDataSource(marshaller: Marshaller, reader: ReaderInterface,
  dirPath: String) extends SpaceDataSource {

  private[this] val logger: Logger = Logger.getLogger(classOf[MarkLogicSpaceDataSource].getCanonicalName())
  private[this] val privateConverter = DocumentObjectConverterInternal.instance()

  //TODO Add namespace
  override def getById(idQuery: DataSourceIdQuery): Object = {
    val typ = idQuery.getTypeDescriptor().getTypeName()
    val id = idQuery.getId().toString
    val uri = XQueryHelper.buildDataUri(dirPath, typ, id)
    val query = " declare default element namespace \"www.simeonot.org\"; doc(\"" + uri + "\")"
    reader.read(query)
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
      //TODO decode xml of SQL
      val sql = query.getAsSQLQuery()

      val toDecodeSQL = GsSqlParser(sql.getQuery())
      val decodedSQL = decodeSQL(toDecodeSQL, sql.getQueryParameters().toList)

      var queryXPath = " declare default element namespace \"www.simeonot.org\"; doc()/" +
        query.getTypeDescriptor().getTypeName()
      if (!decodedSQL.isEmpty) queryXPath = queryXPath + "[" + decodedSQL + "]"

      logger.info(queryXPath)
      new ObjectMLIterator(reader.readMany(queryXPath), marshaller)
    } else null;
  }

  private[this] def decodeSQL(exp: Exp, data: List[Object]): String = {
    exp match {
      case and: And => {
        var counter = 0
        val decoded = and.operands.map(operands => {
          val tempcounter = counter
          counter = counter + operands.requiredNumObjects
          decodeSQL(operands, data.slice(tempcounter, counter))
        })
        val text = decoded.mkString(" and ")
        if (text.isEmpty()) ""
        else "( " + text + " )"
      }
      case or: Or => {
        var counter = 0
        val decoded = or.operands.map(operand => {
          val tempcounter = counter
          counter = counter + operand.requiredNumObjects
          logger.info(data.slice(tempcounter, counter).toString)
          decodeSQL(operand, data.slice(tempcounter, counter))
        })
        val text = decoded.mkString(" or ")
        if (text.isEmpty()) ""
        else "( " + text + " )"
      }
      case in: In => {
        val temp = (data.map(obj => {
          decodeOne(ComparisonType.Equal, obj, in.property)
        })).mkString(" or ")
        if (temp.isEmpty()) ""
        else "( " + temp + " )"
      }
      case eq: Eq => decodeOne(ComparisonType.Equal, data.head, eq.property)
      case noteq: NotEq => decodeOne(ComparisonType.NotEqual, data.head, noteq.property)
      case less: Less => decodeOne(ComparisonType.LessThan, data.head, less.property)
      case lesseq: LessEq => decodeOne(ComparisonType.LessEqual, data.head, lesseq.property)
      case gt: Greater => decodeOne(ComparisonType.GreaterThan, data.head, gt.property)
      case gq: GreaterEq => decodeOne(ComparisonType.GreaterEqual, data.head, gq.property)
      case _ => "true" //skiping like/notlike
    }
  }

  private[this] def decodeOne(comparisonType: ComparisonType, obj: Object, label: String): String = {
    val node = XML.loadString(marshaller.propertyToXML(label, obj))
    val indexFriendly = XmlToXPathDecoder.extractXPath(node, comparisonType).map(xpath => XPath("./" + xpath.xpath))
    logger.info(XmlToXPathDecoder.createAllMatchXpath(indexFriendly))
    XmlToXPathDecoder.createAllMatchXpath(indexFriendly)
  }

  private[this] def queryBasedOnXml(doc: String): ObjectMLIterator = {
    val node = XML.loadString(doc)
    val xpath = XmlToXPathDecoder.extractXPath(node, ComparisonType.Equal)
    val indexFriendlyXpath = xpath.map(currentPath =>
      {
        currentPath.xpath.replaceFirst(" " + node.label, " .")
      })

    var query = " declare default element namespace \"www.simeonot.org\"; doc()/" + node.label
    if (!indexFriendlyXpath.isEmpty) query = query + "[" + indexFriendlyXpath.mkString(" and ") + "]"

    logger.info(query)
    new ObjectMLIterator(reader.readMany(query), marshaller)
  }

  override def getDataIteratorByIds(arg0: DataSourceIdsQuery): DataIterator[Object] = {
    // TODO Auto-generated method stub
    super.getDataIteratorByIds(arg0);
  }

  override def initialDataLoad(): DataIterator[Object] = {
    logger.info("InitialDataLoad called.")
    val dir = XQueryHelper.buildDataDir(dirPath)
    val query = "xdmp:directory(\"" + dir + "\",\"infinity\")"
    new ObjectMLIterator(reader.readMany(query), marshaller)
  }

  override def initialMetadataLoad(): DataIterator[SpaceTypeDescriptor] = {
    logger.info("InitialMetadataLoad called.")
    val dir = XQueryHelper.buildSpaceTypeDir(dirPath)
    val query = "xdmp:directory(\"" + dir + "\",\"1\")"
    new SpaceDescriptorMLIterator(reader.readMany(query))
  }

  override def supportsInheritance(): Boolean = false

}
