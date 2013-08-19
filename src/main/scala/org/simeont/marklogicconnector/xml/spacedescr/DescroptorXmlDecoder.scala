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
package org.simeont.marklogicconnector.xml.spacedescr

import scala.xml.Node
import scala.xml.XML
import com.gigaspaces.document.SpaceDocument
import com.gigaspaces.metadata.StorageType
import com.gigaspaces.metadata.SpaceDocumentSupport
import com.gigaspaces.metadata.index.SpaceIndexType
import com.gigaspaces.annotation.pojo.FifoSupport
import com.thoughtworks.xstream.XStream

object DescroptorXmlDecoder extends XmlNames {

  private[this] val xstream: XStream = new XStream

  val additionalAttributeStringValuesKey = "additionalAttributeStringValues"
  val additionalBooleanValuesKey = "additionalBooleanValues"

  //Extracts all the data stored in a xml SpaceTypeDescriptor
  def decodeXmlOfSpaceType(xmlS: String): Map[String, AnyRef] = {
    val xml: Node = XML.loadString(xmlS)
    val attributes = xml.attributes.asAttrMap
    val id = attributes.get(idAttName).get
    val auto = attributes.get(autoGenerateIdAttName).get.toBoolean
    val routing = attributes.get(routingAttName).get

    var map = Map[String, AnyRef]()
    map = map + ((typeName, attributes.get(typeName).get))
    map = map + ((superTypeName, attributes.get(superTypeName).get))
    map = map + ((typeSimpleName, attributes.get(typeSimpleName).get))

    map = decodeAdditionalAttributes(attributes, map)

    var indexSeq: Seq[IndexHolder] = null
    var fifoPaths: java.util.Set[String] = null
    var fixedProp: Seq[FixedHolder] = null
    var documentWrapperClass: Class[SpaceDocument] = null

    xml.child.foreach(node => {
      if (node.label == fifoGroupingIndexesPathsElName)
        fifoPaths = xstream.fromXML(node.child(0).buildString(true)) match { case set: java.util.Set[String] => set }
      if (node.label == fixedPropsElName)
        fixedProp = node.child.map(childNode => decodeFixedPropFromXml(childNode))
      //Cannot place indexes due to id/rout problem
      if (node.label == indexPropsElName) indexSeq = decodeIndexsFromXml(node.child)
      if (node.label == documentWrapperClassElName)
        documentWrapperClass =
          xstream.fromXML(node.child(0).buildString(true)) match { case x: Class[SpaceDocument] => x }
    })

    map = map + (("additionalElValues", AdditionalElValues(fifoPaths, documentWrapperClass, null)))
    map = map + ((fixedPropsElName, fixedProp))
    map = map + ((indexPropsElName, indexSeq.filterNot(p => p.key == id || p.key == routing)))

    indexSeq.foreach(i => {
      if (i.key == id) {
        map = map + ((idAttName, IdHolder(id, auto, i.index)))
      }
      if (i.key == routing) {
        map = map + ((routingAttName, RoutingHolder(routing, i.index)))
      }
    })

    map
  }

  private[this] def decodeAdditionalAttributes(attributes: Map[String, String],
    map: Map[String, AnyRef]): Map[String, AnyRef] = {

    val additionalBooleanValues = AdditionalBooleanValues(attributes.get(dynamicPropertiesAttName).get.toBoolean,
      attributes.get(optimisticLockingAttName).get.toBoolean, attributes.get(replicableAttName).get.toBoolean)

    val fifiGroupP =
      if (attributes.get(fifoGroupingPropertyPathAttName).get == "null") null
      else attributes.get(fifoGroupingPropertyPathAttName).get
    val additionalAttributeStringValues = AdditionalAttributeStringValues(
      StorageType.valueOf(attributes.get(storageTypeAttName).get),
      FifoSupport.valueOf(attributes.get(fifoSupportAttName).get),
      fifiGroupP)
    map + ((additionalAttributeStringValuesKey, additionalAttributeStringValues),
      (additionalBooleanValuesKey, additionalBooleanValues))
  }

  private[this] def decodeFixedPropFromXml(node: Node): FixedHolder = {
    val attributes = node.attributes.asAttrMap
    val typOfFix = xstream.fromXML(node.child(0).child(0).buildString(true)) match { case x: Class[_] => x }
    val storageType = StorageType.valueOf(attributes.get("storageType").get)
    val documentSupport = SpaceDocumentSupport.valueOf(attributes.get("documentSupport").get)
    val name = attributes.get("name").get
    FixedHolder(name, typOfFix, storageType, documentSupport)
  }

  private[this] def decodeIndexsFromXml(indexSeq: Seq[Node]): Seq[IndexHolder] = {
    indexSeq.map(child => {
      val attributes = child.attributes.asAttrMap
      val id: String = attributes.get("name").get
      val indexType = SpaceIndexType.valueOf(attributes.get("type").get)
      IndexHolder(id, indexType, id.contains('.'))
    })
  }

}

/**
 * Helper class to decode data in xml file
 */
case class IdHolder(key: String, auto: Boolean, index: SpaceIndexType)
/**
 * Helper class to decode data in xml file
 */
case class RoutingHolder(key: String, index: SpaceIndexType)
/**
 * Helper class to decode data in xml file
 */
case class IndexHolder(key: String, index: SpaceIndexType, pathKey: Boolean)
/**
 * Helper class to decode data in xml file
 */
case class FixedHolder(key: String, typ: Class[_], storage: StorageType, docSupport: SpaceDocumentSupport)
/**
 * Helper class to decode data in xml file
 */
case class AdditionalBooleanValues(dynamicProperties: Boolean, optimisticLocking: Boolean, replicable: Boolean)
/**
 * Helper class to decode data in xml file
 */
case class AdditionalAttributeStringValues(storageType: StorageType,
  fifoSupport: FifoSupport, fifoGroupingPropertyPath: String)
/**
 * Helper class to decode data in xml file
 */
case class AdditionalElValues(fifoGroupingIndexesPaths: java.util.Set[String],
  documentWrapperClass: Class[com.gigaspaces.document.SpaceDocument], objectClass: Class[_])
