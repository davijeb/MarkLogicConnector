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
import java.util.HashMap
import java.util.logging.Logger

object SpaceTypeDescriptorMarshaller {

  private[this] val logger: Logger = Logger.getLogger("SpaceTypeDescriptorMarshaller")

  private[this] val xstream: XStream = new XStream

  private[this] val commentFriendlyQuote = "\" "
  private[this] val commentFriendlyEqAndQuote = "=\""
  private[this] val typeName = "type"
  private[this] val superTypeName = "superType"
  private[this] val typeSimpleName = "typeSimple"
  private[this] val idAttName = "id"
  private[this] val routingAttName = "routing"
  private[this] val autoGenerateIdAttName = "autoGenerateId"
  private[this] val dynamicPropertiesAttName = "dynamicProperties"
  private[this] val storageTypeAttName = "storageType"
  private[this] val fifoSupportAttName = "fifoSupport"
  private[this] val fifoGroupingPropertyPathAttName = "fifoGroupingPropertyPath"
  private[this] val replicableAttName = "replicable"
  private[this] val optimisticLockingAttName = "optimisticLocking"
  private[this] val fixedPropsElName = "fixedProperties"
  private[this] val indexPropsElName = "indexes"
  private[this] val fifoGroupingIndexesPathsElName = "fifoGroupingIndexesPaths"
  private[this] val documentWrapperClassElName = "documentWrapperClass"
  private[this] val objectClassElName = "objectClass"

  /**
   * Marshals single SpaceTypeDescriptor
   */
  def marshallSpaceDesc(desc: SpaceTypeDescriptor): String = {
    val head = "<spacedesc " +
      superTypeName + commentFriendlyEqAndQuote + desc.getSuperTypeName + commentFriendlyQuote +
      typeName + commentFriendlyEqAndQuote + desc.getTypeName + commentFriendlyQuote +
      typeSimpleName + commentFriendlyEqAndQuote + desc.getTypeSimpleName + commentFriendlyQuote +
      idAttName + commentFriendlyEqAndQuote + desc.getIdPropertyName + commentFriendlyQuote +
      routingAttName + commentFriendlyEqAndQuote + desc.getRoutingPropertyName + commentFriendlyQuote +
      dynamicPropertiesAttName + commentFriendlyEqAndQuote + desc.supportsDynamicProperties + commentFriendlyQuote +
      optimisticLockingAttName + commentFriendlyEqAndQuote + desc.supportsOptimisticLocking + commentFriendlyQuote +
      autoGenerateIdAttName + commentFriendlyEqAndQuote + desc.isAutoGenerateId + commentFriendlyQuote +
      replicableAttName + commentFriendlyEqAndQuote + desc.isReplicable + commentFriendlyQuote +
      storageTypeAttName + commentFriendlyEqAndQuote + desc.getStorageType + commentFriendlyQuote +
      fifoGroupingPropertyPathAttName + commentFriendlyEqAndQuote +
      desc.getFifoGroupingPropertyPath + commentFriendlyQuote +
      fifoSupportAttName + commentFriendlyEqAndQuote + desc.getFifoSupport + commentFriendlyQuote +
      "concreteType=\"" + desc.isConcreteType() + "\">"

    val end = "</spacedesc>"

    val fifoGroupingIndexesPaths =
      "<" + fifoGroupingIndexesPathsElName + ">" + xstream.toXML(desc.getFifoGroupingIndexesPaths()) +
        "</" + fifoGroupingIndexesPathsElName + ">"

    val documentWrapperClass =
      "<" + documentWrapperClassElName + ">" + xstream.toXML(desc.getDocumentWrapperClass()) +
        "</" + documentWrapperClassElName + ">"

    val objectClass =
      "<" + objectClassElName + ">" + xstream.toXML(desc.getObjectClass()) +
        "</" + objectClassElName + ">"

    val fixedProperties: String =
      "<" + fixedPropsElName + ">" +
        (for (i <- 0 until desc.getNumOfFixedProperties())
          yield fixedPropertyToXml(desc.getFixedProperty(i))).mkString +
        "</" + fixedPropsElName + ">"

    val indexProperties: String =
      "<" + indexPropsElName + ">" +
        desc.getIndexes().map(pair => indexToXml(pair._2)).mkString +
        "</" + indexPropsElName + ">"

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
    var decodedXmls = nodes.map(decodeXmlOfSpaceType(_))

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

  //Extracts all the data stored in a xml SpaceTypeDescriptor
  private[this] def decodeXmlOfSpaceType(xmlS: String): Map[String, AnyRef] = {
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
    map + (("additionalAttributeStringValues", additionalAttributeStringValues),
      ("additionalBooleanValues", additionalBooleanValues))
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

}
