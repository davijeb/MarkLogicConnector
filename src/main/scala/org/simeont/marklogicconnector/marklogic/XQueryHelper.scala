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

object XQueryHelper {

  private[this] val xmlExt = ".xml"

  private[this] val spacetype = "/spacetypedescriptor"

  private[this] val slash = "/"

  def buildDataUri(partialDir: String, clas: String, id: String): String =
    partialDir + slash + clas + slash + id + xmlExt

  def buildSpaceTypeUri(partialDir: String, typ: String): String =
    spacetype + partialDir + slash + typ + xmlExt

  def buildDataDir(partialDir: String): String =
    partialDir + slash

  def buildSpaceTypeDir(partialDir: String): String =
    spacetype + partialDir + slash

  def buildDirectoryQuerigXQuery(dir: String, depth: String): String =
    "xdmp:directory(\"" + dir + "\",\"" + depth + "\")"

  def builDocumentQueringXQuery(namespace: String, url: String, root: String, xpath: String): String =
    addDefaultNamespace(namespace) + addDoc(url) + addRootElement(root) + addXPath(xpath)

  private[this] def addDoc(url: String): String =
    if (url.isEmpty || url.startsWith("(")) " doc(" + url + ")"
    else " doc(\"" + url + "\")"

  private[this] def addDefaultNamespace(namespace: String): String =
    if (namespace != "") "declare default element namespace \"" + namespace + "\";"
    else ""

  private[this] def addRootElement(root: String): String =
    if (root != "") "/" + root
    else ""

  private[this] def addXPath(xpath: String): String =
    if (xpath != "") "[" + xpath + "]"
    else ""
}
