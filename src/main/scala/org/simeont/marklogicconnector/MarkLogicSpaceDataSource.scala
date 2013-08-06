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
package org.simeont.marklogicconnector

import com.gigaspaces.datasource.DataIterator
import com.gigaspaces.datasource.DataSourceIdQuery
import com.gigaspaces.datasource.DataSourceIdsQuery
import com.gigaspaces.datasource.DataSourceQuery
import com.gigaspaces.datasource.SpaceDataSource
import com.gigaspaces.metadata.SpaceTypeDescriptor
import org.simeont.marklogicconnector.xml.Marshaller
import org.simeont.marklogicconnector.marklogic.ReaderInterface
import org.simeont.marklogicconnector.iterators.SpaceDescriptorMLIterator
import org.simeont.marklogicconnector.iterators.ObjectMLIterator

class MarkLogicSpaceDataSource(marshaller: Marshaller, reader: ReaderInterface,
  dirPath: String) extends SpaceDataSource {

  private[this] val xmlExt = ".xml"

  override def getById(idQuery: DataSourceIdQuery): Object = {
    val typ = idQuery.getTypeDescriptor().getTypeName()
    val id = idQuery.getId().toString

    val query = "doc(\"" + dirPath + "/" + typ + "/" + id + xmlExt + "\")"
    reader.read(query)
  }

  override def getDataIterator(query: DataSourceQuery): DataIterator[Object] = {
    // TODO Auto-generated method stub
    super.getDataIterator(query);
  }

  override def getDataIteratorByIds(arg0: DataSourceIdsQuery): DataIterator[Object] = {
    // TODO Auto-generated method stub
    super.getDataIteratorByIds(arg0);
  }

  override def initialDataLoad(): DataIterator[Object] = {
    val query = "xdmp:directory(\"" + dirPath + "/\",\"infinity\")"
    new ObjectMLIterator(reader.readMany(query),marshaller)
  }

  override def initialMetadataLoad(): DataIterator[SpaceTypeDescriptor] = {
    val query = "xdmp:directory(\"/spacedescriptors/" + dirPath + "/\",1)"
    new SpaceDescriptorMLIterator(reader.readMany(query))
  }

  override def supportsInheritance(): Boolean = false
}
