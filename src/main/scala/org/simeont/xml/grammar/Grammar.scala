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
package org.simeont.xml.grammar

/**
 * Grammar to be used to translate objects-XML should implement this trait
 *
 * @author Tomo Simeonov
 */
trait Grammar {

  /**
   * Use the grammar to marshal object to XML
   *
   * @param propertyName The property name this object is associated with
   *
   * @param obj An object to be marshaled
   *
   * @return MarshalledEntry wrapper that describes how the object is translated to XML and what is the type that needs
   *  to be used to regenerated it
   */
  def useGrammarToMarshall(propertyName: String, obj: AnyRef): MarshalledEntry

  /**
   * Use the grammar to regenerate object from XML
   *
   * @param propertyName The property name this XML is associated with
   *
   * @param typ The type this XML represent
   *
   * @param body The actual XML
   *
   * @return Object regenerated from XML
   */
  def useGrammarToUnMarshall(propertyName: String, typ: String, body: String): AnyRef

}

/**
 *  Helper object that wraps around an object translation to XML and the Java/Scala type associated with the XML.
 *
 * @author Tomo Simeonov
 */
case class MarshalledEntry(typ: String, xmlRep: String)
