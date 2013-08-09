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
package org.simeont.marklogicconnector.xml.tests

import org.scalatest.FunSuite
import com.gigaspaces.metadata.SpaceTypeDescriptor
import com.gigaspaces.document.SpaceDocument
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.gigaspaces.metadata.SpaceDocumentSupport
import com.gigaspaces.metadata.StorageType
import com.gigaspaces.metadata.index.SpaceIndex
import com.gigaspaces.metadata.index.SpaceIndexType
import org.simeont.marklogicconnector.xml.SpaceTypeDescriptorMarshaller
import scala.xml.Node
import scala.xml.XML
import com.gigaspaces.annotation.pojo.FifoSupport
import com.gigaspaces.annotation.pojo.SpaceClass
import com.thoughtworks.xstream.XStream

class SpaceTypeDescriptorMarshallerTests extends FunSuite {

  val standardSpaceDescBuilder = new SpaceTypeDescriptorBuilder("Test")
    .idProperty("id", false)
    .addFixedProperty("fix1", "java.lang.String", SpaceDocumentSupport.COPY, StorageType.OBJECT)
    .addPathIndex("id.id", SpaceIndexType.EXTENDED)
    .addPropertyIndex("rounting", SpaceIndexType.EXTENDED)
    .replicable(false)
    .supportsDynamicProperties(false)
    .supportsOptimisticLocking(false)
    .storageType(StorageType.DEFAULT)

  test("should marshall SpaceTypeDescriptor to xml") {
    val builder = standardSpaceDescBuilder

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(
      builder.routingProperty("routing", SpaceIndexType.EXTENDED).create)
    XML.loadString(marshalled)

  }

  test("should unmarshall xml to SpaceTypeDescriptor") {
    val spacedesc = standardSpaceDescBuilder.create

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spacedesc)
    compareSpaceTypes(SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(Array(marshalled)).next, spacedesc)
  }

  test("should unmarshall xml to SpaceTypeDescriptor without id and routing property") {
    val spacedesc = new SpaceTypeDescriptorBuilder("noid").create

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spacedesc)
    compareSpaceTypes(SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(Array(marshalled)).next, spacedesc)
  }

  test("should unmarshall xml to SpaceTypeDescriptor with SuperSpaceType") {
    val sup = standardSpaceDescBuilder.create
    val spacedesc = new SpaceTypeDescriptorBuilder("Test2", sup)
      .addPathIndex("id.id2", SpaceIndexType.EXTENDED)
      .create

    val supMarshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(sup)
    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spacedesc)
    val unMarshalled = SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(Array(supMarshalled, marshalled))
    unMarshalled.next
    compareSpaceTypes(unMarshalled.next, spacedesc)
  }

  test("should unmarshall xml to SpaceTypeDescriptor with Fifo ON") {
    val fifoOn = standardSpaceDescBuilder
<<<<<<< HEAD
      .fifoSupport(FifoSupport.DEFAULT)
      .fifoGroupingProperty("id")
      .addFifoGroupingIndex("id")
      .create
=======
    .fifoSupport(FifoSupport.DEFAULT)
    .fifoGroupingProperty("id")
    .addFifoGroupingIndex("id")
    .create
>>>>>>> More test

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(fifoOn)
    compareSpaceTypes(SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(Array(marshalled)).next, fifoOn)
  }

<<<<<<< HEAD
  test("should unmarshall xml to SpaceTypeDescriptor using class") {
    val desc = new SpaceTypeDescriptorBuilder(classOf[Data], null).create

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(desc)
    compareSpaceTypes(SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(Array(marshalled)).next, desc)
  }

  test("should unmarshall xml to SpaceTypeDescriptor using class and inheritence") {
    val desc = new SpaceTypeDescriptorBuilder(classOf[Data], null).create
    val innerDesc = new SpaceTypeDescriptorBuilder(classOf[DataChild], desc).create

    val iterator = SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(Array(
      SpaceTypeDescriptorMarshaller.marshallSpaceDesc(desc),
      SpaceTypeDescriptorMarshaller.marshallSpaceDesc(innerDesc)))
    val next = iterator.next
    val last = iterator.next
    
    if (next.getTypeName() == "org.simeont.marklogicconnector.xml.tests.Data") {
      compareSpaceTypes(next, desc)
      compareSpaceTypes(last, innerDesc)
    } else {
      compareSpaceTypes(last, desc)
      compareSpaceTypes(next, innerDesc)
    }
  }
=======
>>>>>>> More test

  private[this] def compareSpaceTypes(t1: SpaceTypeDescriptor, t2: SpaceTypeDescriptor): Unit = {
    assert(t1.getDocumentWrapperClass() === t2.getDocumentWrapperClass())
    assert(t1.getFifoGroupingIndexesPaths() === t2.getFifoGroupingIndexesPaths())
    assert(t1.getFifoGroupingPropertyPath() === t2.getFifoGroupingPropertyPath())
    assert(t1.getFifoSupport() === t2.getFifoSupport)
    assert(t1.getIdPropertyName() === t2.getIdPropertyName)
    assert(t1.getIndexes() === t2.getIndexes)
    assert(t1.getNumOfFixedProperties() === t2.getNumOfFixedProperties)
    assert(t1.getObjectClass() === t2.getObjectClass)
    assert(t1.getRoutingPropertyName() == t2.getRoutingPropertyName)
    assert(t1.getStorageType() === t2.getStorageType)
    assert(t1.getSuperTypeName() === t2.getSuperTypeName)
    assert(t1.getTypeName() === t2.getTypeName)
    assert(t1.getTypeSimpleName() === t2.getTypeSimpleName)
    assert(t1.isAutoGenerateId() === t2.isAutoGenerateId)
    assert(t1.isConcreteType() === t2.isConcreteType)
    assert(t1.isReplicable() === t2.isReplicable)
    assert(t1.supportsDynamicProperties() === t2.supportsDynamicProperties)
    assert(t1.supportsOptimisticLocking() === t2.supportsOptimisticLocking)
  }
  
}

@SpaceClass class Data()
@SpaceClass class DataChild() extends Data
