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

import scala.xml.Node
import scala.xml.XML
import org.simeont.marklogicconnector.sql.parser.ComparisonType._
import org.simeont.marklogicconnector.xml.Marshaller

object XmlToXPathDecoder {

  def createAllMatchXpath(xpaths: List[XPath]): String = {
    if (!xpaths.isEmpty) "(" + xpaths.map(_.xpath).mkString(" and ") + ")"
    else ""
  }

  def extractXPath(node: Node, comparisonType: ComparisonType): List[XPath] = {
    val comparison = comparisonType match {
      case ComparisonType.Equal | ComparisonType.IN => "eq"
      case ComparisonType.GreaterEqual => ">="
      case ComparisonType.GreaterThan => ">"
      case ComparisonType.LessEqual => "<="
      case ComparisonType.LessThan => "<"
      case ComparisonType.NotEqual => "!="
      case _ => "eq"
    }
    decoder(node, comparison)
  }

  private[this] val TEXTNODE =  "#PCDATA"
  private[this] def decoder(node: Node, comparison: String): List[XPath] = {
    val masterLabel = node.label
    if (node.child.size == 1 && node.child(0).label == TEXTNODE) {
      List(XPath(masterLabel + biulldAttributes(node, comparison) + "[. " + comparison + " " +
        typeSafeValue(node.text) + "]"))
    } else {
      val notCombined = node.descendant.flatMap(f => {
        val hasText = f.child.size == 1 && f.child(0).label == TEXTNODE
        val text = if (hasText) Some(f.text) else None
        if (f.label != TEXTNODE)
          List(PrivateXPath(f.label + biulldAttributes(f, comparison),
            f.child.filter(_.label != TEXTNODE).map(_.label),
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

      notCombined.filter(_.ready).map(f => XPath(masterLabel + "/" + f.label + "[. " + comparison + " " +
        typeSafeValue(f.value.get) + "]"))
    }
  }

  private[this] def biulldAttributes(node: Node, comparison: String): String = {
    val transaltedAttr =
      node.attributes.asAttrMap.filterNot(_._1.startsWith("xs:"))
        .map(attribute => "(@" + attribute._1 + " " + comparison + " " + typeSafeValue(attribute._2) + ")")

    if (!transaltedAttr.isEmpty)
      "[" + transaltedAttr.mkString(" and ") + "]"
    else ""
  }

  private[this] def typeSafeValue(obj: String): String =
    obj match {
      case x: String if (x.matches("(-)?[0-9]+(.)?[0-9]+") || x.matches("(-)?[0-9]+")) => x
      case s: String => "\"" + s + "\""
    }

  case class PrivateXPath(var label: String, childs: Seq[String], value: Option[String], ready: Boolean)
}

case class XPath(xpath: String)
