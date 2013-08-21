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

import java.util.HashMap
import java.util.logging.Logger
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

object SpaceTypeDescriptorMarshaller extends XmlNames {

  private[this] val logger: Logger = Logger.getLogger("SpaceTypeDescriptorMarshaller")

  private[this] val xstream: XStream = new XStream

  /**
   * Marshals single SpaceTypeDescriptor
   */
  def marshallSpaceDesc(desc: SpaceTypeDescriptor): String =
    new DescriptorXmlBuilder()
      .addSuperType(desc.getSuperTypeName)
      .addType(desc.getTypeName)
      .addSimpleType(desc.getTypeSimpleName)
      .addId(desc.getIdPropertyName)
      .addRouting(desc.getRoutingPropertyName)
      .addDynamicProp(desc.supportsDynamicProperties.toString)
      .addOptimisticLocking(desc.supportsOptimisticLocking.toString)
      .addAutoGenerateId(desc.isAutoGenerateId.toString)
      .addReplicable(desc.isReplicable.toString)
      .addStorageType(desc.getStorageType.toString)
      .addFifoGrouping(desc.getFifoGroupingPropertyPath)
      .addFifoSupport(desc.getFifoSupport.toString)
      .addFifoGroupingIndexesPaths(xstream.toXML(desc.getFifoGroupingIndexesPaths()))
      .addDocumentWrapperClass(xstream.toXML(desc.getDocumentWrapperClass()))
      .addObjectClass(xstream.toXML(desc.getObjectClass()))
      .addFixedProperties((for (i <- 0 until desc.getNumOfFixedProperties())
        yield fixedPropertyToXml(desc.getFixedProperty(i))).mkString)
      .addIndexProperties(desc.getIndexes().map(pair => indexToXml(pair._2)).mkString)
      .buildXml

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

  /**
   * Marshals single SpaceIndex so that can be added to the xml document
   */
  def indexToXml(spaceIndex: SpaceIndex): String =
    "<index name=\"" + spaceIndex.getName() + "\" type=\"" + spaceIndex.getIndexType() + "\"/>"

  /**
   * Unmarshalls all SpaceTypeDescriptors at one go, this is how it manage to keep inheritance
   */
  def unmarshallAllSpaceDesc(nodes: Array[String]): Iterator[SpaceTypeDescriptor] = {
    var currentSpaceTypesMap = Map[String, SpaceTypeDescriptor]()
    var decodedXmls = nodes.map(DescroptorXmlDecoder.decodeXmlOfSpaceType(_))

    while (!decodedXmls.isEmpty) {
      decodedXmls.foreach(m => {
        if (currentSpaceTypesMap.containsKey(m(superTypeName))) {
          val superType = currentSpaceTypesMap(m(superTypeName).toString)
          currentSpaceTypesMap = currentSpaceTypesMap + ((
              m(typeName).toString, unmarshallSpaceDesc(m, Some(superType))))
        }
        if (m(superTypeName) == "java.lang.Object")
          currentSpaceTypesMap = currentSpaceTypesMap + ((m(typeName).toString, unmarshallSpaceDesc(m, None)))
      })

      decodedXmls = decodedXmls.filterNot(m => currentSpaceTypesMap.containsKey(m(typeName)))
    }

    currentSpaceTypesMap.values.toIterator
  }

  //Unmarshals single SpaceTypeDesc without super type
  private[this] def unmarshallSpaceDesc(builderValues: Map[String, AnyRef],
    superType: Option[SpaceTypeDescriptor]): SpaceTypeDescriptor = {

    val typName: String = builderValues(typeName) match { case x: String => x; case _ => throw new ClassCastException }
    val clas: Option[Class[_]] = tryToFindClass(typName)
    if (clas.isDefined) {
      DescriptorBuilder.newDescriptor(clas.get, superType)
        .addIndex(builderValues.get(indexPropsElName))
        .build
    } else {
      DescriptorBuilder.newDescriptor(typName, superType)
        .addId(builderValues.get(idAttName))
        .addRouting(builderValues.get(routingAttName))
        .addFixed(builderValues.get(fixedPropsElName))
        .addIndex(builderValues.get(indexPropsElName))
        .addAdditionalBooleanValues(builderValues.get("additionalBooleanValues"))
        .addAdditionalAttributeStringValues(builderValues.get("additionalAttributeStringValues"))
        .addAdditionalElValues(builderValues.get("additionalElValues"))
        .build
    }
  }

  private[this] def tryToFindClass(typName: String): Option[Class[_]] = {
    try {
      val mar = xstream.fromXML("<java-class>" + typName + "</java-class>")
      Some(mar match { case x: Class[_] => x; case _ => throw new ClassCastException })
    } catch {
      case x: Throwable => None
    }
  }

}
