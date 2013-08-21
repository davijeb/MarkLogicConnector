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
import scala.collection.JavaConversions._
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.gigaspaces.metadata.SpaceTypeDescriptor
import com.gigaspaces.metadata.index.SpaceIndex

object DescriptorBuilder {
  def newDescriptor(typName: String, superType: SpaceTypeDescriptor): DescriptorBuilder =
    new DescriptorBuilder(new SpaceTypeDescriptorBuilder(typName, superType), Some(superType))

  def newDescriptor(typName: String): DescriptorBuilder =
    new DescriptorBuilder(new SpaceTypeDescriptorBuilder(typName), None)

  def newDescriptor(clas: Class[_], superType: SpaceTypeDescriptor): DescriptorBuilder =
    new DescriptorBuilder(new SpaceTypeDescriptorBuilder(clas, superType), Some(superType))

  def newDescriptor(clas: Class[_]): DescriptorBuilder =
    new DescriptorBuilder(new SpaceTypeDescriptorBuilder(clas, null), None)

  def newDescriptor(clas: Class[_], superType: Option[SpaceTypeDescriptor]): DescriptorBuilder =
    if (superType.isEmpty) newDescriptor(clas)
    else newDescriptor(clas, superType.get)

  def newDescriptor(typName: String, superType: Option[SpaceTypeDescriptor]): DescriptorBuilder =
    if (superType.isEmpty) newDescriptor(typName)
    else newDescriptor(typName, superType.get)
}

class DescriptorBuilder(var descriptorBuilder: SpaceTypeDescriptorBuilder,
  superType: Option[SpaceTypeDescriptor]) extends XmlNames {

  def addId(id: Option[AnyRef]): DescriptorBuilder = {
    val superCheck = if (superType.isDefined) superType.get.getIdPropertyName() == null else true
    if (id.isDefined && superCheck) {
      val idHolder: IdHolder = id.get match { case x: IdHolder => x; case _ => throw new ClassCastException }
      descriptorBuilder = descriptorBuilder.idProperty(idHolder.key, idHolder.auto, idHolder.index)
    }
    this
  }

  def addRouting(routing: Option[AnyRef]): DescriptorBuilder = {
    val superCheck = if (superType.isDefined) superType.get.getRoutingPropertyName() == null else true
    if (routing.isDefined && superCheck) {
      val rHolder: RoutingHolder = routing.get match { case x: RoutingHolder => x }
      descriptorBuilder = descriptorBuilder.routingProperty(rHolder.key, rHolder.index)
    }
    this
  }

  def addFixed(fixedProp: Option[AnyRef]): DescriptorBuilder = {
    if (fixedProp.isDefined) {
      val fixedPropArray: Seq[FixedHolder] =
        fixedProp.get match { case x: Seq[FixedHolder] => x; case _ => throw new ClassCastException }
      fixedPropArray.foreach(fix =>
        if (superType.isEmpty || superType.get.getFixedProperty(fix.key) == null)
          descriptorBuilder = descriptorBuilder.addFixedProperty(fix.key, fix.typ, fix.docSupport, fix.storage))
    }
    this
  }

  def addIndex(indexProp: Option[AnyRef]): DescriptorBuilder = {
    val superIndexs = if (superType.isDefined) superType.get.getIndexes() else new HashMap[String, SpaceIndex]()
    if (indexProp.isDefined) {
      val indexPropArray: Seq[IndexHolder] =
        indexProp.get match { case x: Seq[IndexHolder] => x; case _ => throw new ClassCastException }
      indexPropArray.foreach(ind => {
        if (!superIndexs.containsKey(ind.key))
          if (ind.pathKey) descriptorBuilder = descriptorBuilder.addPathIndex(ind.key, ind.index)
          else descriptorBuilder = descriptorBuilder.addPropertyIndex(ind.key, ind.index)
      })
    }
    this
  }

  def addAdditionalBooleanValues(additionalBooleanValues: Option[AnyRef]): DescriptorBuilder = {
    val additionalBooleanValuesGeted = additionalBooleanValues.get match {
      case x: AdditionalBooleanValues => x; case _ => throw new ClassCastException
    }

    descriptorBuilder = descriptorBuilder
      .supportsDynamicProperties(additionalBooleanValuesGeted.dynamicProperties)
      .supportsOptimisticLocking(additionalBooleanValuesGeted.optimisticLocking)
      .replicable(additionalBooleanValuesGeted.replicable)
    this
  }

  def addAdditionalAttributeStringValues(additionalAttStringValues: Option[AnyRef]): DescriptorBuilder = {
    val additionalAttributeStringValues = additionalAttStringValues.get match {
      case x: AdditionalAttributeStringValues => x; case _ => throw new ClassCastException
    }

    descriptorBuilder = descriptorBuilder.fifoSupport(additionalAttributeStringValues.fifoSupport)

    val superCheckStorage = if (superType.isDefined) superType.get.getStorageType == null else true
    if (superCheckStorage)
      descriptorBuilder = descriptorBuilder.storageType(additionalAttributeStringValues.storageType)

    val superCheckFifo = if (superType.isDefined) superType.get.getFifoGroupingPropertyPath == null else true

    if (superCheckFifo && additionalAttributeStringValues.fifoGroupingPropertyPath != null)
      descriptorBuilder =
        descriptorBuilder.fifoGroupingProperty(additionalAttributeStringValues.fifoGroupingPropertyPath)

    this
  }

  def addAdditionalElValues(additionalValues: Option[AnyRef]): DescriptorBuilder = {
    val additionalElValues = additionalValues.get match {
      case x: AdditionalElValues => x; case _ => throw new ClassCastException
    }

    descriptorBuilder = descriptorBuilder.documentWrapperClass(additionalElValues.documentWrapperClass)
    additionalElValues.fifoGroupingIndexesPaths.foreach(
      i => descriptorBuilder = descriptorBuilder.addFifoGroupingIndex(i))

    this
  }

  def build = descriptorBuilder.create()
}
