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
package org.simeont.marklogicconnector.xml

import scala.collection.JavaConversions._
import scala.xml.Node
import scala.xml.XML
import com.gigaspaces.document.SpaceDocument
import org.simeont.marklogicconnector.xml.grammar.Grammar
import org.simeont.marklogicconnector.xml.grammar.BasicGrammar
import com.gigaspaces.internal.document.DocumentObjectConverterInternal

/**
 * Basic Marshaller implemented using basic grammar object
 *
 * @author Tomo Simeonov
 */
class BasicMarshaller() extends Marshaller {

  private[this] var grammar: Grammar = new BasicGrammar

  private[this] val privateConverter = DocumentObjectConverterInternal.instance()
  
  def setGrammar(grammar : Grammar) = this.grammar = grammar

  private[this] val docType = "SpaceDocument"
  private[this] val openTag = "<"
  private[this] val endTag = ">"
  private[this] val closedOpenTag = "</"
  private[this] val closedEndTag = "/>"
  private[this] val xs = " xmlns:xs=\"http://www.w3.org/2001/XMLSchema\""

  /**
   * Marshals SpaceDocument to XML string using the grammar provided
   */
  def toXML(document: SpaceDocument): String = toXML(document,true)

  private[this] def toXML(document: SpaceDocument, addXS : Boolean) : String = {
    openTag + document.getTypeName + {if(addXS) xs else ""} + endTag +
      (document.getProperties().map(x => propertyToXML(x._1, x._2))).mkString +
      closedOpenTag + document.getTypeName + endTag
  }

  /**
   * Marshals single property of SpaceDocument to XML string using the grammar provided
   */
  def propertyToXML(name: String, obj: AnyRef): String = {
    obj match {
      case spDoc: SpaceDocument =>
        constructElement(name, docType, toXML(spDoc,false))
      case o: AnyRef => {
        val entry = grammar.useGrammarToMarshall(name, o)
        constructElement(name, entry.typ, entry.xmlRep)
      }
    }
  }

  //Helper method to create xml element for a property
  private[this] def constructElement(name: String, typ: String, xml: String) = openTag + name  + " xs:type=\"" +
  typ + "\"" + endTag + xml + closedOpenTag + name + endTag

  /**
   * Marshals XML string to SpaceDocument using the grammar provided
   */
  def fromXML(xmlS: String): SpaceDocument = {
    val xml: Node = XML.loadString(xmlS)
    val sp: SpaceDocument = new SpaceDocument(xml.label)
    xml.child.foreach(el => {
      val propertyName = el.label
      val typ = el.attributes.value(0).text
      val body = el.child(0).buildString(true)

      if (typ == docType) {
        val property = tryToConvert(fromXML(body)).get
        sp.setProperty(propertyName, property) 
      } else {
        sp.setProperty(propertyName, grammar.useGrammarToUnMarshall(propertyName, typ, body))
      }

    })

    sp

  }

  private[this] def tryToConvert(prop : SpaceDocument) : Option[Any] = {
    try{
     Some(privateConverter.toObject(prop))
    }catch{case x : Throwable => {println(x.getMessage());Some(prop)}}
    
  }
}
