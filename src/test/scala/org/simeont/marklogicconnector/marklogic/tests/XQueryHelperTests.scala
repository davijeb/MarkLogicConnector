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
package org.simeont.marklogicconnector.marklogic.tests

import org.scalatest.FunSuite
import org.simeont.marklogicconnector.marklogic.XQueryHelper

class XQueryHelperTests extends FunSuite {

  test("should build correct data and spacedescriptor dirs") {
    assert(XQueryHelper.buildDataDir("/test") === "/test/")
    assert(XQueryHelper.buildSpaceTypeDir("/test") === "/spacetypedescriptor/test/")
  }

  test("should build correct data uris") {
    assert(XQueryHelper.buildDataUri("/test", "Person", "2") === "/test/Person/2.xml")

  }

  test("should build correct spacetypedescriptor uris") {
    assert(XQueryHelper.buildSpaceTypeUri("/test", "Person") === "/spacetypedescriptor/test/Person.xml")

  }

  test("should build dir open query") {
    assert(XQueryHelper.buildDirectoryQuerigXQuery("/test/", "infinity") === "xdmp:directory(\"/test/\",\"infinity\")")
  }

  test("should build document open query with only url") {
    assert(XQueryHelper.builDocumentQueringXQuery("", "test.xml", "", "") === " doc(\"test.xml\")")
  }

  test("should build document open query with only namespace and url") {
    assert(XQueryHelper.builDocumentQueringXQuery("www.simeont.org", "(\"test.xml\",\"test2.xml\")", "", "") ===
      "declare default element namespace \"www.simeont.org\"; doc((\"test.xml\",\"test2.xml\"))")
  }
  test("should build document open query without xpath") {
    assert(XQueryHelper.builDocumentQueringXQuery("www.simeont.org", "test.xml", "root", "") ===
      "declare default element namespace \"www.simeont.org\"; doc(\"test.xml\")/root")
  }

  test("should build document open query") {
    assert(XQueryHelper.builDocumentQueringXQuery("www.simeont.org", "test.xml", "root", "(./a[. eq \"b\")") ===
      "declare default element namespace \"www.simeont.org\"; doc(\"test.xml\")/root[(./a[. eq \"b\")]")
  }
  
  test("should build document open query without url"){
    assert(XQueryHelper.builDocumentQueringXQuery("www.simeont.org", "", "root", "(./a[. eq \"b\")") ===
      "declare default element namespace \"www.simeont.org\"; doc()/root[(./a[. eq \"b\")]")
  }
}
