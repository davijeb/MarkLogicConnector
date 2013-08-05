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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.SpaceDataSource
import com.gigaspaces.metadata.SpaceTypeDescriptor;

class MarkLogicSpaceDataSource extends SpaceDataSource {

  override def getById(idQuery: DataSourceIdQuery): Object = {
    // TODO Auto-generated method stub
    super.getById(idQuery);
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
    // TODO Auto-generated method stub
    super.initialDataLoad();
  }

  override def initialMetadataLoad(): DataIterator[SpaceTypeDescriptor] = {
    // TODO Auto-generated method stub
    super.initialMetadataLoad();
  }

  override def supportsInheritance(): Boolean = {
    super.supportsInheritance();
  }
}
