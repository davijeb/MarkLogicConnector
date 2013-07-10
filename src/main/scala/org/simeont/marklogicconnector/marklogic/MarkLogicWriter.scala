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
package org.simeont.marklogicconnector.marklogic

import com.marklogic.xcc.ContentSource
import com.marklogic.xcc.Session
import com.marklogic.xcc.Request
import com.marklogic.xcc.RequestOptions
import com.marklogic.xcc.Content
import org.simeont.marklogicconnector.batch.ProcecessedOperationActionHolder

/**
 *
 */
class MarkLogicWriter(contentSource: ContentSource, nameSpace : String) extends WriterInterface {

  //TODO check if it works
  def persistAll(batchHolder: ProcecessedOperationActionHolder) {
    val session = contentSource.newSession()
    session.setTransactionMode(Session.TransactionMode.UPDATE)
    try {
 
      session.insertContent(batchHolder.contents.getOrElse(Array[Content]()))
    
      if(batchHolder.doDelete){
        val delete = session.newAdhocQuery(batchHolder getDeleteXqueryCode nameSpace)
        session.submitRequest(delete)
      }

      if(batchHolder.doUpdate){
        val update = session.newAdhocQuery(batchHolder getUpdateXqueryCode nameSpace)
        session.submitRequest(update)
      }

      session.commit()
   
    } catch {
      case x: Throwable => { session.rollback(); throw x }
    }
  }
}
