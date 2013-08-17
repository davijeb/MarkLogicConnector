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
package org.simeont.marklogicconnector.sql.parser

import scala.xml.XML
import org.simeont.marklogicconnector.xml.Marshaller
import org.simeont.marklogicconnector.sql.parser.ComparisonType._

class GsSqlDecoder(marshaller: Marshaller) {

  def decodeSQL(exp: Exp, data: List[Object]): String = {
    exp match {
      case and: And => decodeConditionOperation(and, data)
      case or: Or => decodeConditionOperation(or, data)
      case in: In => {
        val temp = (data.map(obj => {
          decodeOne(ComparisonType.Equal, obj, in.property)
        })).mkString(" or ")
        if (temp.isEmpty() || temp == "") ""
        else "( " + temp + " )"
      }
      case eq: Eq => decodeOne(ComparisonType.Equal, data.head, eq.property)
      case noteq: NotEq => decodeOne(ComparisonType.NotEqual, data.head, noteq.property)
      case less: Less => decodeOne(ComparisonType.LessThan, data.head, less.property)
      case lesseq: LessEq => decodeOne(ComparisonType.LessEqual, data.head, lesseq.property)
      case gt: Greater => decodeOne(ComparisonType.GreaterThan, data.head, gt.property)
      case gq: GreaterEq => decodeOne(ComparisonType.GreaterEqual, data.head, gq.property)
      case nt : Nothing => "" //Nothing can be only valid if no where clause
      case _ => "true" //TODO skiping like/notlike for now
    }
  }

  private[this] def decodeConditionOperation(exp: ConditionOperation, data: List[Object]): String = {
    var counter = 0
    val decoded = exp.contained.map(operand => {
      val tempcounter = counter
      counter = counter + operand.requiredNumObjects
      decodeSQL(operand, data.slice(tempcounter, counter))
    })

    val text = exp match {
      case a: And => decoded.mkString(" and ")
      case o: Or => decoded.mkString(" or ")
    }

    if (text.isEmpty() || text == "") ""
    else "( " + text + " )"
  }

  private[this] def decodeOne(comparisonType: ComparisonType, obj: Object, label: String): String = {
    val node = XML.loadString(marshaller.propertyToXML(label, obj))
    val indexFriendly = XmlToXPathDecoder.extractXPath(node, comparisonType).map(xpath => XPath("./" + xpath.xpath))
    XmlToXPathDecoder.createAllMatchXpath(indexFriendly)
  }
}
