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

trait XmlNames {

  val commentFriendlyQuote = "\" "
  val commentFriendlyEqAndQuote = "=\""
  val typeName = "type"
  val superTypeName = "superType"
  val typeSimpleName = "typeSimple"
  val idAttName = "id"
  val routingAttName = "routing"
  val autoGenerateIdAttName = "autoGenerateId"
  val dynamicPropertiesAttName = "dynamicProperties"
  val storageTypeAttName = "storageType"
  val fifoSupportAttName = "fifoSupport"
  val fifoGroupingPropertyPathAttName = "fifoGroupingPropertyPath"
  val replicableAttName = "replicable"
  val optimisticLockingAttName = "optimisticLocking"
  val fixedPropsElName = "fixedProperties"
  val indexPropsElName = "indexes"
  val fifoGroupingIndexesPathsElName = "fifoGroupingIndexesPaths"
  val documentWrapperClassElName = "documentWrapperClass"
  val objectClassElName = "objectClass"

}
