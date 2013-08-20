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

/**
 * 	Helper class to build XML from SpaceTypeDescriptor information
 */
class DescriptorXmlBuilder() extends XmlNames {

  private[this] var superType: String = ""
  private[this] var typ: String = ""
  private[this] var simpleType: String = ""
  private[this] var id: String = ""
  private[this] var routing: String = ""
  private[this] var dynamicProp: String = ""
  private[this] var optimisticLocking: String = ""
  private[this] var autoGenerateId: String = ""
  private[this] var replicable: String = ""
  private[this] var storageType: String = ""
  private[this] var fifoGrouping: String = ""
  private[this] var fifoSupport: String = ""
  private[this] var concreteType: String = ""
  private[this] var fifoGroupingIndexesPaths: String = ""
  private[this] var documentWrapperClass: String = ""
  private[this] var objectClass: String = ""
  private[this] var fixedProperties: String = ""
  private[this] var indexProperties: String = ""

  private[this] val end = "</spacedesc>"

  def addSuperType(superType: String): DescriptorXmlBuilder = {
    this.superType = superTypeName + commentFriendlyEqAndQuote + superType + commentFriendlyQuote
    this
  }

  def addType(typ: String): DescriptorXmlBuilder = {
    this.typ = typeName + commentFriendlyEqAndQuote + typ + commentFriendlyQuote
    this
  }

  def addSimpleType(simpleType: String): DescriptorXmlBuilder = {
    this.simpleType = typeSimpleName + commentFriendlyEqAndQuote + simpleType + commentFriendlyQuote
    this
  }

  def addId(id: String): DescriptorXmlBuilder = {
    this.id = idAttName + commentFriendlyEqAndQuote + id + commentFriendlyQuote
    this
  }

  def addRouting(routing: String): DescriptorXmlBuilder = {
    this.routing = routingAttName + commentFriendlyEqAndQuote + routing + commentFriendlyQuote
    this
  }

  def addDynamicProp(dynamicProp: String): DescriptorXmlBuilder = {
    this.dynamicProp = dynamicPropertiesAttName + commentFriendlyEqAndQuote + dynamicProp + commentFriendlyQuote
    this
  }

  def addOptimisticLocking(optimisticLocking: String): DescriptorXmlBuilder = {
    this.optimisticLocking =
      optimisticLockingAttName + commentFriendlyEqAndQuote + optimisticLocking + commentFriendlyQuote
    this
  }

  def addAutoGenerateId(autoGenerateId: String): DescriptorXmlBuilder = {
    this.autoGenerateId = autoGenerateIdAttName + commentFriendlyEqAndQuote + autoGenerateId + commentFriendlyQuote
    this
  }

  def addReplicable(replicable: String): DescriptorXmlBuilder = {
    this.replicable = replicableAttName + commentFriendlyEqAndQuote + replicable + commentFriendlyQuote
    this
  }

  def addStorageType(storageType: String): DescriptorXmlBuilder = {
    this.storageType = storageTypeAttName + commentFriendlyEqAndQuote + storageType + commentFriendlyQuote
    this
  }

  def addFifoGrouping(fifoGrouping: String): DescriptorXmlBuilder = {
    this.fifoGrouping =
      fifoGroupingPropertyPathAttName + commentFriendlyEqAndQuote + fifoGrouping + commentFriendlyQuote
    this
  }

  def addFifoSupport(fifoSupport: String): DescriptorXmlBuilder = {
    this.fifoSupport = fifoSupportAttName + commentFriendlyEqAndQuote + fifoSupport + commentFriendlyQuote
    this
  }

  def addConcreteType(concreteType: String): DescriptorXmlBuilder = {
    this.concreteType = "concreteType=\"" + concreteType + "\""
    this
  }

  def addFifoGroupingIndexesPaths(fifoGroupingIndexesPaths: String): DescriptorXmlBuilder = {
    this.fifoGroupingIndexesPaths = "<" + fifoGroupingIndexesPathsElName + ">" + fifoGroupingIndexesPaths +
      "</" + fifoGroupingIndexesPathsElName + ">"
    this
  }

  def addDocumentWrapperClass(documentWrapperClass: String): DescriptorXmlBuilder = {
    this.documentWrapperClass = "<" + documentWrapperClassElName + ">" + documentWrapperClass +
      "</" + documentWrapperClassElName + ">"
    this
  }
  
  def addObjectClass(objectClass: String): DescriptorXmlBuilder = {
    this.objectClass = "<" + objectClassElName + ">" + objectClass + "</" + objectClassElName + ">"
    this
  }
  
  def addFixedProperties(fixedProperties: String): DescriptorXmlBuilder = {
    this.fixedProperties = "<" + fixedPropsElName + ">" + fixedProperties + "</" + fixedPropsElName + ">"
    this
  }
  
  def addIndexProperties(indexProperties: String): DescriptorXmlBuilder = {
    this.indexProperties = "<" + indexPropsElName + ">" + indexProperties + "</" + indexPropsElName + ">"
    this
  }
  
  private[this] def header: String = {
    "<spacedesc " + superType + typ + simpleType + id + routing + dynamicProp + optimisticLocking + autoGenerateId +
      replicable + storageType + fifoGrouping + fifoSupport + concreteType + ">"
  }

  def buildXml: String ={
    header + fifoGroupingIndexesPaths + documentWrapperClass + objectClass + fixedProperties + indexProperties + end
  }
}
