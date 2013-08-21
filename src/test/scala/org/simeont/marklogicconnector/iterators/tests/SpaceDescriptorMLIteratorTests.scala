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
package org.simeont.marklogicconnector.iterators.tests

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.marklogic.xcc.ResultSequence
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.gigaspaces.annotation.pojo.SpaceClass
import org.simeont.marklogicconnector.iterators.SpaceDescriptorMLIterator
import com.gigaspaces.metadata.SpaceTypeDescriptor
import org.simeont.marklogicconnector.xml.spacedescr.SpaceTypeDescriptorMarshaller

class SpaceDescriptorMLIteratorTests extends FunSuite with MockitoSugar {

  val descClass = new SpaceTypeDescriptorBuilder(classOf[Data], null).create
  val innerDescClass = new SpaceTypeDescriptorBuilder(classOf[DataChild], descClass).create
  val descClassMarshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(descClass)
  val innerDescClassMarshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(innerDescClass)

  val descDocument = new SpaceTypeDescriptorBuilder("Test", null).create
  val innerDescDocument = new SpaceTypeDescriptorBuilder("Test2", descDocument).create
  val descDocumentMarshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(descDocument)
  val innerDescMarshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(innerDescDocument)

  val emptyResultArray: Array[String] = Array()
  val oneItemClassResultArray = Array(descClassMarshalled)
  val oneItemDocResultArray = Array(descDocumentMarshalled)
  val manyItemsResultArray = Array(innerDescMarshalled, descClassMarshalled,
    descDocumentMarshalled, innerDescClassMarshalled)

  test("should not crash when there are no SpaceTypeDescriptors for the iterator") {
    val testSpecificResultSequence = mock[ResultSequence]
    when(testSpecificResultSequence.asStrings).thenReturn(emptyResultArray)

    val iterator = new SpaceDescriptorMLIterator(testSpecificResultSequence)
    assert(iterator.hasNext() === false)
     assert(iterator.next() === null)
  }

  test("should return one SpaceTypeDescriptors when there is only one returned from ML") {
    val testSpecificResultSequence = mock[ResultSequence]
    when(testSpecificResultSequence.asStrings).thenReturn(oneItemClassResultArray)

    var iterator = new SpaceDescriptorMLIterator(testSpecificResultSequence)
    assert(iterator.hasNext() === true)
    compareSpaceTypes(iterator.next(), descClass)

    when(testSpecificResultSequence.asStrings).thenReturn(oneItemDocResultArray)
    iterator = new SpaceDescriptorMLIterator(testSpecificResultSequence)
    assert(iterator.hasNext() === true)
    compareSpaceTypes(iterator.next(), descDocument)

    assert(iterator.next() === null)
  }

  test("should return all SpaceTypeDescriptors when there are many returned from ML") {
    val testSpecificResultSequence = mock[ResultSequence]
    when(testSpecificResultSequence.asStrings).thenReturn(manyItemsResultArray)

    val iterator = new SpaceDescriptorMLIterator(testSpecificResultSequence)
    val answer = Array(iterator.next, iterator.next, iterator.next, iterator.next)
    answer.foreach(f => {
      if (f.getTypeName() == descDocument.getTypeName()) compareSpaceTypes(f, descDocument)
      if (f.getTypeName() == innerDescDocument.getTypeName()) compareSpaceTypes(f, innerDescDocument)
      if (f.getTypeName() == descClass.getTypeName()) compareSpaceTypes(f, descClass)
      if (f.getTypeName() == innerDescClass.getTypeName()) compareSpaceTypes(f, innerDescClass)
    })
  }

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
