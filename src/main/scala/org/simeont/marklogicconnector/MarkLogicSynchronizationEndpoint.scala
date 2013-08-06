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

import java.util.logging.Logger
import scala.collection.mutable.{ Map => MMap }
import com.gigaspaces.sync.AddIndexData
import com.gigaspaces.sync.ConsolidationParticipantData
import com.gigaspaces.sync.IntroduceTypeData
import com.gigaspaces.sync.OperationsBatchData
import com.gigaspaces.sync.SpaceSynchronizationEndpoint
import com.gigaspaces.sync.TransactionData
import com.gigaspaces.sync.DataSyncOperationType
import org.simeont.marklogicconnector.batch.action._
import org.simeont.marklogicconnector.factory.CustomContentFactory
import org.simeont.marklogicconnector.marklogic.WriterInterface
import org.simeont.marklogicconnector.batch.ProcecessedOperationActionHolder
import org.simeont.marklogicconnector.batch.OperatinoActionProcessor
import org.simeont.marklogicconnector.xml.SpaceDescriptorMarshaller

class MarkLogicSynchronizationEndpoint(customContentFactory: CustomContentFactory, writer: WriterInterface,
  dirPath: String) extends SpaceSynchronizationEndpoint {

  private[this] val logger: Logger = Logger.getLogger(classOf[MarkLogicSynchronizationEndpoint].getCanonicalName())

  private[this] val xmlExt = ".xml"
  /*
   * SpaceTypeDescriptor related persistence
   */
  override def onIntroduceType(introduceTypeData: IntroduceTypeData) = {
    val typeXML = SpaceDescriptorMarshaller marshallSpaceDesc introduceTypeData.getTypeDescriptor()
    val uri = "/spacedescriptors/" + dirPath  + introduceTypeData.getTypeDescriptor().getTypeName() + xmlExt
    writer.persistSpaceDescriptor(customContentFactory.generateContent(uri, typeXML))

  }

  override def onAddIndex(addIndexData: AddIndexData) = {
    val uri = "/spacedescriptors/" + dirPath + addIndexData.getTypeName() + xmlExt
    addIndexData.getIndexes().foreach(index =>
      writer.addElementToDocument(uri, "/spacedesc/indexes", SpaceDescriptorMarshaller indexToXml (index)))
  }

  /*
   * Batch and Transaction persistence
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
          val typ = entry.getTypeDescriptor().getTypeName()
          val idField = entry.getTypeDescriptor().getIdPropertyName()
          val idValue = (entry.getDataAsDocument().getProperty(idField)).toString
          val uid = dirPath + "/" + typ + "/" + idValue + xmlExt

          entry.getDataSyncOperationType() match {
            case DataSyncOperationType.WRITE | DataSyncOperationType.UPDATE =>
              OperatinoActionProcessor.add(InsertAction(uid, entry.getDataAsDocument), operatinoDataMap)
            case DataSyncOperationType.REMOVE =>
              OperatinoActionProcessor.add(DeleteAction(uid), operatinoDataMap)
            case DataSyncOperationType.PARTIAL_UPDATE =>
              OperatinoActionProcessor.add(UpdateAction(uid, entry.getDataAsDocument), operatinoDataMap)
            case DataSyncOperationType.REMOVE_BY_UID => () //TODO
            case DataSyncOperationType.CHANGE => () //TODO
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

  /*
   * After synchronization
   */
  override def afterOperationsBatchSynchronization(batchData: OperationsBatchData) = {
    // TODO Auto-generated method stub
    super.afterOperationsBatchSynchronization(batchData)

  }

  override def afterTransactionSynchronization(transactionData: TransactionData) = {
    // TODO Auto-generated method stub
    super.afterTransactionSynchronization(transactionData);
  }
}
