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
package org.simeont.marklogicconnector.iterators

import java.util.logging.Logger
import java.lang.Throwable
import com.gigaspaces.datasource.DataIterator
import com.marklogic.xcc.ResultSequence
import com.marklogic.xcc.ResultItem
import org.simeont.marklogicconnector.xml.SpaceDescriptorMarshaller
import com.gigaspaces.metadata.SpaceTypeDescriptor
import org.simeont.marklogicconnector.xml.Marshaller

class ObjectMLIterator(resultSequence: ResultSequence, xmlMarshaller: Marshaller)
  extends MLIterator[Object](resultSequence) {

  override def fromXml(item: ResultItem): Object =
    xmlMarshaller.fromXML(item.getItem().asString())
}
