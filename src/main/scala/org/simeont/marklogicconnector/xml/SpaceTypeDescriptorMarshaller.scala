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
package org.simeont.marklogicconnector.xml

import scala.collection.mutable.{ Map => MMap }
import scala.xml.Node
import scala.xml.XML
import scala.collection.JavaConversions._
import com.gigaspaces.metadata.SpaceTypeDescriptor
import com.thoughtworks.xstream.XStream
import com.gigaspaces.metadata.index.SpaceIndex
import com.gigaspaces.metadata.SpacePropertyDescriptor
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.gigaspaces.metadata.StorageType
import com.gigaspaces.annotation.pojo.FifoSupport
import com.gigaspaces.document.SpaceDocument
import com.gigaspaces.metadata.SpaceDocumentSupport
import com.gigaspaces.metadata.index.SpaceIndexType

object SpaceTypeDescriptorMarshaller {

  private[this] val xstream: XStream = new XStream

  private[this] val commentFriendlyQuote = "\" "

  def marshallSpaceDesc(desc: SpaceTypeDescriptor): String = {
    val head = "<spacedesc superType=\"" + desc.getSuperTypeName() + commentFriendlyQuote +
      "type=\"" + desc.getTypeName() + commentFriendlyQuote +
      "typeSimple=\"" + desc.getTypeSimpleName() + commentFriendlyQuote +
      "id=\"" + desc.getIdPropertyName() + commentFriendlyQuote +
      "routing=\"" + desc.getRoutingPropertyName() + commentFriendlyQuote +
      "dynamicProperties=\"" + desc.supportsDynamicProperties() + commentFriendlyQuote +
      "optimisticLocking=\"" + desc.supportsOptimisticLocking() + commentFriendlyQuote +
      "autoGenerateId=\"" + desc.isAutoGenerateId() + commentFriendlyQuote +
      "replicable=\"" + desc.isReplicable() + commentFriendlyQuote +
      "storageType=\"" + desc.getStorageType() + commentFriendlyQuote +
      "fifoGroupingPropertyPath=\"" + desc.getFifoGroupingPropertyPath() + commentFriendlyQuote +
      "fifoSupport=\"" + desc.getFifoSupport() + commentFriendlyQuote +
      "concreteType=\"" + desc.isConcreteType() + "\">"

    val end = "</spacedesc>"

    val fifoGroupingIndexesPaths =
      "<fifoGroupingIndexesPaths>" + xstream.toXML(desc.getFifoGroupingIndexesPaths()) + "</fifoGroupingIndexesPaths>"

    val documentWrapperClass =
      "<documentWrapperClass>" + xstream.toXML(desc.getDocumentWrapperClass()) + "</documentWrapperClass>"

    val objectClass =
      "<objectClass>" + xstream.toXML(desc.getObjectClass()) + "</objectClass>"

    val fixedProperties: String =
      "<fixedProperties>" +
        (for (i <- 0 until desc.getNumOfFixedProperties())
          yield fixedPropertyToXml(desc.getFixedProperty(i))).mkString +
        "</fixedProperties>"

    val indexProperties: String =
      "<indexes>" +
        desc.getIndexes().map(pair => indexToXml(pair._2)).mkString +
        "</indexes>"

    head + fifoGroupingIndexesPaths + fixedProperties + indexProperties + documentWrapperClass + objectClass + end
  }

  private[this] def fixedPropertyToXml(sPropertyDescriptor: SpacePropertyDescriptor): String = {
    val head =
      "<fixProperty name=\"" + sPropertyDescriptor.getName() + commentFriendlyQuote +
        "storageType=\"" + sPropertyDescriptor.getStorageType() + commentFriendlyQuote +
        "documentSupport=\"" + sPropertyDescriptor.getDocumentSupport() + commentFriendlyQuote +
        "typeName=\"" + sPropertyDescriptor.getTypeName() + "\">"
    val end =
      "</fixProperty>"

    val typ =
      "<type>" + xstream.toXML(sPropertyDescriptor.getType()) + "</type>"

    head + typ + end
  }

  def indexToXml(spaceIndex: SpaceIndex): String =
    "<index name=\"" + spaceIndex.getName() + "\" type=\"" + spaceIndex.getIndexType() + "\"/>"

  def unmarshallSpaceDesc(xmlS: String): SpaceTypeDescriptor = {

    val xml: Node = XML.loadString(xmlS)
    val attributes = xml.attributes.asAttrMap
    val id = attributes.get("id").get
    val routing = attributes.get("routing").get
    var typ: SpaceTypeDescriptorBuilder = new SpaceTypeDescriptorBuilder(attributes.get("type").get)
      .supportsDynamicProperties(attributes.get("dynamicProperties").get.toBoolean)
      .supportsOptimisticLocking(attributes.get("optimisticLocking").get.toBoolean)
      .replicable(attributes.get("replicable").get.toBoolean)
      .storageType(StorageType.valueOf(attributes.get("storageType").get))
      .fifoSupport(FifoSupport.valueOf(attributes.get("fifoSupport").get))
    if (attributes.get("fifoGroupingPropertyPath").get != "null")
      typ = typ.addFifoGroupingIndex(attributes.get("fifoGroupingPropertyPath").get)

    var indexMap: MMap[String, SpaceIndexType] = null
    
    xml.child.foreach(node => {
      if (node.label == "fifoGroupingIndexesPaths")
        xstream.fromXML(node.child(0).buildString(true)) match {
          case set: java.util.Set[String] => set.foreach(i => typ = typ.addFifoGroupingIndex(i))
        }
      if (node.label == "fixedProperties")
        node.child.foreach(childNode => typ = addfixedPropFromXml(typ, childNode))
      if (node.label == "indexes")
        indexMap = buildIndexMap(node.child)
      if (node.label == "documentWrapperClass")
        typ = typ.documentWrapperClass(
          xstream.fromXML(node.child(0).buildString(true)) match {
            case x: Class[SpaceDocument] => x
          })
    })

    //This will handle if the routing and id fields have an additional index
    val idIndex = indexMap.getOrElse(id,SpaceIndexType.BASIC)
    indexMap.remove(id)
     val routingIndex = indexMap.getOrElse(routing,SpaceIndexType.BASIC)
    indexMap.remove(routing)
    
    typ = addIndexes(typ,indexMap)
    typ = typ
      .idProperty(id, attributes.get("autoGenerateId").get.toBoolean,idIndex)
      .routingProperty(routing,routingIndex)

    typ.create
  }

  private[this] def addIndexes(typ: SpaceTypeDescriptorBuilder,
    indexes: MMap[String, SpaceIndexType]): SpaceTypeDescriptorBuilder = {
    var tempTyp = typ
    indexes.foreach(indexPair => {
      if (indexPair._1.contains('.'))
        tempTyp = tempTyp.addPathIndex(indexPair._1, indexPair._2)
      else
        tempTyp = tempTyp.addPropertyIndex(indexPair._1, indexPair._2)
    })
    tempTyp
  }

  private[this] def addfixedPropFromXml(typ: SpaceTypeDescriptorBuilder, child: Node): SpaceTypeDescriptorBuilder = {
    val attributes = child.attributes.asAttrMap
    val typOfFix = xstream.fromXML(child.child(0).child(0).buildString(true)) match { case x: Class[_] => x }
    val storageType = StorageType.valueOf(attributes.get("storageType").get)
    val documentSupport = SpaceDocumentSupport.valueOf(attributes.get("documentSupport").get)
    val name = attributes.get("name").get

    typ.addFixedProperty(name, typOfFix, documentSupport, storageType)
  }

  private[this] def buildIndexMap(indexSet: Seq[Node]): MMap[String, SpaceIndexType] = {
    var map = MMap[String, SpaceIndexType]()
    indexSet.foreach(child => {
      val attributes = child.attributes.asAttrMap
      val id: String = attributes.get("name").get
      val indexType = SpaceIndexType.valueOf(attributes.get("type").get)
      if (!map.contains(id) || map.get(id).get == SpaceIndexType.NONE ||
        map.get(id).get == SpaceIndexType.BASIC && indexType == SpaceIndexType.EXTENDED) map.put(id, indexType)
    })
    map
  }
}
