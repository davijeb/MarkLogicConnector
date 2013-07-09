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
package org.simeont

import scala.beans.BeanProperty
import scala.collection.mutable.{ Map => MMap }
import org.springframework.beans.factory.annotation.Autowired
import com.gigaspaces.sync.AddIndexData
import com.gigaspaces.sync.ConsolidationParticipantData
import com.gigaspaces.sync.IntroduceTypeData
import com.gigaspaces.sync.OperationsBatchData
import com.gigaspaces.sync.SpaceSynchronizationEndpoint
import com.gigaspaces.sync.TransactionData
import com.gigaspaces.sync.DataSyncOperationType
import org.simeont.batch.OperatinoActionProcessor
import org.simeont.batch.action._
import org.simeont.factory._
import org.simeont.marklogic.WriterInterface
import org.simeont.batch.ProcecessedOperationActionHolder

class MarkLogicSynchronizationEndpoint(customContentFactory: CustomContentFactory, writer: WriterInterface,
  dirPath: String) extends SpaceSynchronizationEndpoint {

  /*
   * SpaceTypeDescriptor related persistence
   */
  override def onIntroduceType(introduceTypeData: IntroduceTypeData) = {

  }

  override def onAddIndex(addIndexData: AddIndexData) = {
    //add to document
  }

  /*
   * Batch persistence
   */
  override def onOperationsBatchSynchronization(batchData: OperationsBatchData): Unit =
    processOperationData(batchData.getBatchDataItems)

  override def onTransactionSynchronization(transactionData: TransactionData): Unit =
    processOperationData(transactionData.getTransactionParticipantDataItems)

  private def processOperationData(dataItems: Array[com.gigaspaces.sync.DataSyncOperation]): Unit = {
    var transformedOperatinoData: Option[ProcecessedOperationActionHolder] = None
    try {
      val operatinoDataMap = MMap[String, Action]()
      dataItems.foreach(entry =>
        {
          val idField = entry.getTypeDescriptor().getIdPropertyName()
          val idValue = (entry.getDataAsDocument().getProperty(idField)).toString
          val uid = dirPath + "/" + idValue + ".xml"

          entry.getDataSyncOperationType() match {
            case DataSyncOperationType.WRITE | DataSyncOperationType.UPDATE =>
              OperatinoActionProcessor.add(InsertAction(uid, entry.getDataAsDocument), operatinoDataMap)
            case DataSyncOperationType.REMOVE =>
              OperatinoActionProcessor.add(DeleteAction(uid), operatinoDataMap)
            case DataSyncOperationType.PARTIAL_UPDATE =>
              OperatinoActionProcessor.add(UpdateAction(uid, entry.getDataAsDocument), operatinoDataMap)
            case DataSyncOperationType.REMOVE_BY_UID => () //Todo
          }
        })

      transformedOperatinoData = Option(OperatinoActionProcessor.transform(operatinoDataMap, customContentFactory))
      writer persistAll transformedOperatinoData.get
    } catch {
      case ex: Throwable => processFailure(dataItems, transformedOperatinoData, ex)
    }
  }

  /*
   * Processing failures
   */
  def processFailure(batchData: Array[com.gigaspaces.sync.DataSyncOperation],
    transformedOperatinoData: Option[ProcecessedOperationActionHolder], ex: Throwable) = {
    //TODO Do something
    ex.printStackTrace()
  }

  override def onTransactionConsolidationFailure(participantData: ConsolidationParticipantData) = {
    // TODO Auto-generated method stub
    super.onTransactionConsolidationFailure(participantData);
  }

  override def afterOperationsBatchSynchronization(batchData: OperationsBatchData) = {
    // TODO Auto-generated method stub
    super.afterOperationsBatchSynchronization(batchData)

  }

  /*
   * Transaction persistence
   */
  override def afterTransactionSynchronization(transactionData: TransactionData) = {
    // TODO Auto-generated method stub
    super.afterTransactionSynchronization(transactionData);
  }
}
