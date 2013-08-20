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
        if (currentSpaceTypesMap.containsKey(m(superTypeName)))
          currentSpaceTypesMap = currentSpaceTypesMap + ((
            m(typeName).toString, unmarshallSpaceDescWithSuperType(m, currentSpaceTypesMap(m(superTypeName).toString))))
        if (m(superTypeName) == "java.lang.Object")
          currentSpaceTypesMap = currentSpaceTypesMap + ((m(typeName).toString, unmarshallSpaceDesc(m)))
      })

      decodedXmls = decodedXmls.filterNot(m => currentSpaceTypesMap.containsKey(m(typeName)))
    }

    currentSpaceTypesMap.values.toIterator
  }

  //Unmarshals single SpaceTypeDesc without super type
  private[this] def unmarshallSpaceDesc(builderValues: Map[String, AnyRef]): SpaceTypeDescriptor = {

    val typName: String = builderValues(typeName) match { case x: String => x; case _ => throw new ClassCastException }
    val clas: Option[Class[_]] = tryToFindClass(typName)
    if (clas.isDefined) {
      createFromClass(builderValues, null, clas)
    } else {
      var typBuilder: SpaceTypeDescriptorBuilder = new SpaceTypeDescriptorBuilder(typName)
      typBuilder = addId(builderValues, typBuilder, true)
      typBuilder = addRouting(builderValues, typBuilder, true)
      typBuilder = addFixeds(builderValues, null, typBuilder)
      typBuilder = addAdditional(builderValues, null, typBuilder)
      typBuilder = addIndexs(builderValues, typBuilder, new HashMap[String, SpaceIndex]())

      typBuilder.create
    }
  }

  //Unmarshals single SpaceTypeDesc with super type
  private[this] def unmarshallSpaceDescWithSuperType(builderValues: Map[String, AnyRef],
    superType: SpaceTypeDescriptor): SpaceTypeDescriptor = {

    val typName: String = builderValues(typeName) match { case x: String => x; case _ => throw new ClassCastException }
    val clas: Option[Class[_]] = tryToFindClass(typName)
    if (clas.isDefined) {
      createFromClass(builderValues, superType, clas)
    } else {
      var typBuilder: SpaceTypeDescriptorBuilder = new SpaceTypeDescriptorBuilder(typName, superType)
      typBuilder = addId(builderValues, typBuilder, superType.getIdPropertyName == null)
      typBuilder = addRouting(builderValues, typBuilder, superType.getRoutingPropertyName == null)
      typBuilder = addFixeds(builderValues, superType, typBuilder)
      typBuilder = addIndexs(builderValues, typBuilder, superType.getIndexes)
      typBuilder = addAdditional(builderValues, superType, typBuilder)

      typBuilder.create
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

  private[this] def createFromClass(builderValues: Map[String, AnyRef], superType: SpaceTypeDescriptor,
    clas: Option[Class[_]]): SpaceTypeDescriptor = {
    var typBuilder: SpaceTypeDescriptorBuilder = new SpaceTypeDescriptorBuilder(clas.get, superType)
    val temTyp = new SpaceTypeDescriptorBuilder(clas.get, superType)
    typBuilder = addIndexs(builderValues, typBuilder, temTyp.create.getIndexes())
    typBuilder.create
  }

  private[this] def addId(builderValues: Map[String, AnyRef],
    typBuilder: SpaceTypeDescriptorBuilder, additionalCheck: Boolean): SpaceTypeDescriptorBuilder = {
    val idFromMap = builderValues.get(idAttName)
    if (additionalCheck && idFromMap.isDefined) {
      val idHolder: IdHolder = idFromMap.get match { case x: IdHolder => x; case _ => throw new ClassCastException }
      typBuilder.idProperty(idHolder.key, idHolder.auto, idHolder.index)

    } else
      typBuilder
  }

  private[this] def addRouting(builderValues: Map[String, AnyRef], typBuilder: SpaceTypeDescriptorBuilder,
    additionalCheck: Boolean): SpaceTypeDescriptorBuilder = {
    val routingFromMap = builderValues.get(routingAttName)
    if (additionalCheck && routingFromMap.isDefined) {
      val rHolder: RoutingHolder = routingFromMap.get match { case x: RoutingHolder => x }
      typBuilder.routingProperty(rHolder.key, rHolder.index)
    } else
      typBuilder
  }

  private[this] def addFixeds(builderValues: Map[String, AnyRef], superType: SpaceTypeDescriptor,
    typBuilder: SpaceTypeDescriptorBuilder): SpaceTypeDescriptorBuilder = {
    var tempTypBuilder = typBuilder
    val fixedProp = builderValues.get(fixedPropsElName)
    if (!fixedProp.isEmpty) {
      val fixedPropArray: Seq[FixedHolder] =
        fixedProp.get match { case x: Seq[FixedHolder] => x; case _ => throw new ClassCastException }
      fixedPropArray.foreach(fix =>
        if (superType == null || superType.getFixedProperty(fix.key) == null)
          tempTypBuilder = tempTypBuilder.addFixedProperty(fix.key, fix.typ, fix.docSupport, fix.storage))
    }
    tempTypBuilder
  }

  private[this] def addIndexs(builderValues: Map[String, AnyRef], typBuilder: SpaceTypeDescriptorBuilder,
    superIndexs: java.util.Map[String, SpaceIndex]): SpaceTypeDescriptorBuilder = {
    var tempTypBuilder = typBuilder
    val indexProp = builderValues.get(indexPropsElName)
    if (!indexProp.isEmpty) {
      val indexPropArray: Seq[IndexHolder] =
        indexProp.get match { case x: Seq[IndexHolder] => x; case _ => throw new ClassCastException }
      indexPropArray.foreach(ind => {
        if (!superIndexs.containsKey(ind.key))
          if (ind.pathKey) tempTypBuilder = tempTypBuilder.addPathIndex(ind.key, ind.index)
          else {
            tempTypBuilder = tempTypBuilder.addPropertyIndex(ind.key, ind.index)
          }
      })
    }
    tempTypBuilder
  }

  private[this] def addAdditional(builderValues: Map[String, AnyRef], superType: SpaceTypeDescriptor,
    typBuilder: SpaceTypeDescriptorBuilder): SpaceTypeDescriptorBuilder = {
    var tempTypBuilder = typBuilder
    val additionalBooleanValues = builderValues("additionalBooleanValues") match {
      case x: AdditionalBooleanValues => x; case _ => throw new ClassCastException
    }
    tempTypBuilder = tempTypBuilder
      .supportsDynamicProperties(additionalBooleanValues.dynamicProperties)
      .supportsOptimisticLocking(additionalBooleanValues.optimisticLocking)
      .replicable(additionalBooleanValues.replicable)

    val additionalAttributeStringValues = builderValues("additionalAttributeStringValues") match {
      case x: AdditionalAttributeStringValues => x; case _ => throw new ClassCastException
    }

    tempTypBuilder = tempTypBuilder.fifoSupport(additionalAttributeStringValues.fifoSupport)
    if (superType == null || superType.getStorageType == null)
      tempTypBuilder = tempTypBuilder.storageType(additionalAttributeStringValues.storageType)

    if ((superType == null || superType.getFifoGroupingPropertyPath() == null) &&
      additionalAttributeStringValues.fifoGroupingPropertyPath != null)
      tempTypBuilder = tempTypBuilder.fifoGroupingProperty(additionalAttributeStringValues.fifoGroupingPropertyPath)

    val additionalElValues = builderValues("additionalElValues") match {
      case x: AdditionalElValues => x; case _ => throw new ClassCastException
    }

    tempTypBuilder = tempTypBuilder.documentWrapperClass(additionalElValues.documentWrapperClass)
    additionalElValues.fifoGroupingIndexesPaths.foreach(i => tempTypBuilder = tempTypBuilder.addFifoGroupingIndex(i))

    tempTypBuilder
  }

}
