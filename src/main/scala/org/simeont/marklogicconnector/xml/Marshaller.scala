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

import com.gigaspaces.document.SpaceDocument

/**
 * Trait that needs to be implement in order to create a Marshaller which will handle
 * the transformation SpaceDocument-XML
 *
 * @author Tomo Simeonov
 */
trait Marshaller {

  /**
   * Marshals a SpaceDocument to XML string
   *
   */
  def toXML(document: SpaceDocument): String

  /**
   * Marshals single property of the SpaceDocument
   *
   * @param name The property name
   * @param obj The object associated with this property, can be other SpaceDocument
   * @return The XML representation of the property
   */
  def propertyToXML(name: String, obj: AnyRef): String

  /**
   * Restores SpaceDocument from its XML representation
   *
   * @param xml The XML to be translated to SpaceDocument
   */
  def fromXML(xml: String): SpaceDocument
}
