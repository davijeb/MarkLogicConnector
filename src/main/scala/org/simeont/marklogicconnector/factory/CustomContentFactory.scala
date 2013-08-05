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
package org.simeont.marklogicconnector.factory

import com.gigaspaces.document.SpaceDocument
import com.marklogic.xcc.Content
import com.marklogic.xcc.ContentFactory
import com.marklogic.xcc.ContentPermission
import com.marklogic.xcc.ContentCreateOptions
import org.simeont.marklogicconnector.xml.Marshaller
import org.simeont.marklogicconnector.xml.BasicMarshaller

/**
 * Factory for creating a Content object with provided uri and xml. The ContentCreateOption object is supplemented
 * from this factory based on the properties provided to it on initialization
 *
 * xmlMarshaller property - The marshaller to be used, by default uses the basic one
 *
 * namespace property - The default namespace of the documents stored
 *
 * collections property - The collections that need to be assigned to the documents, single string each collection
 * separate by a comma
 *
 * role property - The role of the MarkLogic user, used to create permissions for the document
 */
class CustomContentFactory() {

  private[this] var xmlMarshaller: Marshaller = new BasicMarshaller

  private[this] var namespace: String = ""

  private[this] var collections: Array[String] = Array()

  private[this] var permissions: Array[ContentPermission] = Array()

  def setNamespace(namespace: String) = this.namespace = namespace

  def setXmlMarshaller(xmlMarshaller: Marshaller) = this.xmlMarshaller = xmlMarshaller

  def setCollections(collections: String): Unit = this.collections = collections.split(",")

  def setRole(role: String): Unit = {
    permissions = Array(
      ContentPermission.newInsertPermission(role),
      ContentPermission.newReadPermission(role),
      ContentPermission.newUpdatePermission(role))
  }

  def generateContent(uri: String, document: SpaceDocument): Content = {
    val xml = xmlMarshaller.toXML(document)
    ContentFactory.newContent(uri, xml, provideContentCreateOptions)

  }

  def generateContent(uri: String, xml : String): Content = {
    ContentFactory.newContent(uri, xml, provideContentCreateOptions)
  }

  private[this] var contentCreateOptions: Option[ContentCreateOptions] = None

  private[this] def provideContentCreateOptions: ContentCreateOptions = {
    contentCreateOptions match {
      case Some(cCO) => cCO
      case None => {
        if (namespace.isEmpty || collections.isEmpty || permissions.isEmpty){
          throw new IllegalArgumentException("CustomContentFactory is not provided with all arguments")
        }
        val tempContentCreateOptions = new ContentCreateOptions()
        tempContentCreateOptions.setNamespace(namespace)
        tempContentCreateOptions.setPermissions(permissions)
        tempContentCreateOptions.setCollections(collections)
        contentCreateOptions = Some(tempContentCreateOptions)
        tempContentCreateOptions
      }
    }
  }
}
