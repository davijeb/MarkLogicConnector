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

class SpaceTypeDescriptorMarshallerTests extends FunSuite {

  test("should marshall SpaceTypeDescriptor to xml") {
    val spacedesc = new SpaceTypeDescriptorBuilder("Test")
      .idProperty("id")
      .routingProperty("rounting")
      .addFixedProperty("fix1", "java.lang.String", SpaceDocumentSupport.COPY, StorageType.OBJECT)
      .addPropertyIndex("routing", SpaceIndexType.EXTENDED)
      .addPathIndex("id.id", SpaceIndexType.EXTENDED)
      .replicable(false)
      .supportsDynamicProperties(false)
      .supportsOptimisticLocking(false)
      .storageType(StorageType.DEFAULT)
      .create

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spacedesc)
  }

  test("should unmarshall xml to SpaceTypeDescriptor") {
    val spacedesc = new SpaceTypeDescriptorBuilder("Test")
      .idProperty("id", false)
      .routingProperty("rounting")
      .addFixedProperty("fix1", "java.lang.String", SpaceDocumentSupport.COPY, StorageType.OBJECT)
      .addPropertyIndex("routing", SpaceIndexType.EXTENDED)
      .addPathIndex("id.id", SpaceIndexType.EXTENDED)
      .replicable(false)
      .supportsDynamicProperties(false)
      .supportsOptimisticLocking(false)
      .storageType(StorageType.DEFAULT)
      .create

    val marshalled = SpaceTypeDescriptorMarshaller.marshallSpaceDesc(spacedesc)
    SpaceTypeDescriptorMarshaller.unmarshallSpaceDesc(marshalled)
  }

  test("should unmarshall xml with FifoSupport null") {

  }

}

