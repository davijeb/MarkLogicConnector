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

object SpaceDescriptorMarshaller {

  private[this] val xstream: XStream = new XStream

  def marshallSpaceDesc(desc: SpaceTypeDescriptor): String = {
    val head = "<spacedesc superType=\"" + desc.getSuperTypeName() + "\" " +
      "type=\"" + desc.getTypeName() + "\" " +
      "typeSimple=\"" + desc.getTypeSimpleName() + "\" " +
      "id=\"" + desc.getIdPropertyName() + "\" " +
      "routing=\"" + desc.getRoutingPropertyName() + "\" " +
      "dynamicProperties=\"" + desc.supportsDynamicProperties() + "\" " +
      "optimisticLocking=\"" + desc.supportsOptimisticLocking() + "\" " +
      "autoGenerateId=\"" + desc.isAutoGenerateId() + "\" " +
      "replicable=\"" + desc.isReplicable() + "\" " +
      "storageType=\"" + desc.getStorageType() + "\" " +
      "fifoGroupingPropertyPath=\"" + desc.getFifoGroupingPropertyPath() + "\" " +
      "fifoSupport=\"" + desc.getFifoSupport() + "\" " +
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
      "<fixProperty name=\"" + sPropertyDescriptor.getName() + "\" " +
        "storageType=\"" + sPropertyDescriptor.getStorageType() + "\" " +
        "documentSupport=\"" + sPropertyDescriptor.getDocumentSupport() + "\" " +
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
    var typ: SpaceTypeDescriptorBuilder = new SpaceTypeDescriptorBuilder(attributes.get("type").get)
      .idProperty(id, attributes.get("autoGenerateId").get.toBoolean)
      .routingProperty(attributes.get("routing").get)
      .supportsDynamicProperties(attributes.get("dynamicProperties").get.toBoolean)
      .supportsOptimisticLocking(attributes.get("optimisticLocking").get.toBoolean)
      .replicable(attributes.get("replicable").get.toBoolean)
      .storageType(StorageType.valueOf(attributes.get("storageType").get))
      .fifoSupport(FifoSupport.valueOf(attributes.get("fifoSupport").get))

    if (attributes.get("fifoGroupingPropertyPath").get != "null")
      typ = typ.addFifoGroupingIndex(attributes.get("fifoGroupingPropertyPath").get)

    xml.child.foreach(node => {
      if (node.label == "fifoGroupingIndexesPaths")
        xstream.fromXML(node.child(0).buildString(true)) match {
          case set: java.util.Set[String] => set.foreach(i => typ = typ.addFifoGroupingIndex(i))
        }
      if (node.label == "fixedProperties")
        node.child.foreach(child => typ = addfixedPropFromXml(typ, child))
      if (node.label == "indexes")
        node.child.foreach(child => typ = addIndexFromXml(typ, child,id))
      if (node.label == "documentWrapperClass")
        typ = typ.documentWrapperClass(
          xstream.fromXML(node.child(0).buildString(true)) match {
            case x: Class[SpaceDocument] => x
          })
    })

    typ.create
  }

  private[this] def addfixedPropFromXml(typ: SpaceTypeDescriptorBuilder, child: Node): SpaceTypeDescriptorBuilder = {

    val attributes = child.attributes.asAttrMap
    val typOfFix = xstream.fromXML(child.child(0).child(0).buildString(true)) match { case x: Class[_] => x }
    val storageType = StorageType.valueOf(attributes.get("storageType").get)
    val documentSupport = SpaceDocumentSupport.valueOf(attributes.get("documentSupport").get)
    val name = attributes.get("name").get

    typ.addFixedProperty(name, typOfFix, documentSupport, storageType)
  }

  private[this] def addIndexFromXml(typ: SpaceTypeDescriptorBuilder, child: Node,
    primaryKey: String): SpaceTypeDescriptorBuilder = {
    val attributes = child.attributes.asAttrMap
    val id = attributes.get("name").get
    val indexType = SpaceIndexType.valueOf(attributes.get("type").get)
    if (primaryKey != id || indexType == SpaceIndexType.EXTENDED) {
      val path = id.contains('.')
      if (path)
        typ.addPathIndex(id, indexType)
      else
        typ.addPropertyIndex(id, indexType)
    } else
      typ
  }
}
