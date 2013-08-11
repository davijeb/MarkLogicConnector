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

import org.simeont.marklogicconnector.xml.Marshaller
import scala.xml.Node
import scala.xml.XML
import org.simeont.marklogicconnector.sql.parser.data.ComparisonType._
import scala.xml.Elem
import org.simeont.marklogicconnector.sql.parser.data.ComparisonType

class XmlToXPathDecoder(xmlMarshaller: Marshaller) {

  def extractXPath(name: String, obj: AnyRef, comparisonType: ComparisonType): String = {
    val comparison = comparisonType match {
      case ComparisonType.Equal | ComparisonType.IN => "eq"
      case ComparisonType.GreaterEqual => ">="
      case ComparisonType.GreaterThan => ">"
      case ComparisonType.LessEqual => "<="
      case ComparisonType.LessThan => "<"
      case ComparisonType.NotEqual => "!="
      case _ => "eq"
    }
    val marshalled = xmlMarshaller.propertyToXML(name, obj)
    val node: Node = XML.loadString(marshalled)
    decoder(node, comparison)
  }

  private[this] def decoder(node: Node, comparison: String): String = {
    val notCombined = node.descendant.flatMap(f => {
      val hasText = f.child.size == 1 && f.child(0).label == "#PCDATA"
      val text = if (hasText) Some(f.text) else None
      if (f.label != "#PCDATA")
        List(Path(f.label + biulldAttributes(f, comparison),
          f.child.filter(_.label != "#PCDATA").map(_.label),
          text,
          hasText))
      else List()
    })

    for (i <- 0 until (notCombined.size)) {
      val current = notCombined(i)
      var toUpdate = 0
      var maxToUpdate = current.childs.size
      var j = i + 1
      var toSkip = 0
      while (toUpdate != maxToUpdate && j < notCombined.size) {
        if (toSkip == 0 && notCombined(j).label.startsWith(current.childs(toUpdate))) {
          notCombined(j).label = current.label + "/" + notCombined(j).label
          toUpdate = toUpdate + 1
        }

        if (toSkip > 0) toSkip = toSkip - 1 + notCombined(j).childs.size
        else toSkip = toSkip + notCombined(j).childs.size

        j = j + 1
      }

    }

    val filtered = notCombined.filter(_.ready).map(f => f.label + "[. " + comparison + " \"" + f.value.get + "\"]")

    "(" + filtered.mkString(" and ") + ")"
  }

  private[this] def biulldAttributes(node: Node, comparison: String): String = {
    val transaltedAttr =
      node.attributes.asAttrMap.filterNot(_._1.startsWith("xs:"))
          .map(attribute => "(@" + attribute._1 + " " + comparison + " " + attribute._2 + ")")
      
    if (!transaltedAttr.isEmpty)
      "[" + transaltedAttr.mkString(" and ") + "]"
    else ""
  }

  case class Path(var label: String, childs: Seq[String], value: Option[String], ready: Boolean)
}

