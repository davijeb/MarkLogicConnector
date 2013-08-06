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

import com.gigaspaces.datasource.DataIterator
import com.marklogic.xcc.ResultSequence
import java.util.logging.Logger
import java.lang.Throwable
import com.marklogic.xcc.ResultItem

/**
 * Class that has the commonly used methods between the two iterators for reading from MarkLogic
 */
abstract class MLIterator[T  <: Object](resultSequence: ResultSequence) extends DataIterator[T] {

  private[this] val logger: Logger = Logger.getLogger(classOf[MLIterator[T]].getCanonicalName())

  private[this] val waitTime = 25

  override def close = try {
    resultSequence.close()
  } catch {
    case _: Throwable => logger.warning("Cannont close resultSequence")
  }

  override def hasNext: Boolean = resultSequence.hasNext()

  override def next: T = {
    try {
      val nextItem = resultSequence.next()
      while (!nextItem.isFetchable())
        try {
          Thread.sleep(waitTime)
        } catch {
          case ie: InterruptedException => ()
        }

      fromXml(nextItem)
    } catch {
      case e: Throwable => {
        logger.warning("Error while trying to read resultSequence " + e.getMessage())
        throw e
      }
    }
  }

  override def remove = ()

  /**
   * This method is overridden in the two implementation of this class
   */
  def fromXml(item: ResultItem): T
}
