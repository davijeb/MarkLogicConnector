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
package org.simeont.marklogicconnector.sql.parser.tests

import org.scalatest.FunSuite
import org.simeont.marklogicconnector.sql.parser._

class XmlToXPathDecoderTests extends FunSuite {

  val edgeCaseNode = <test>1</test>
  val simpleNode = <test><n>1</n></test>
  val complexNode = <test><n><a b="2">1</a><b>a</b></n><k/></test>

  test("should extract xpaths from edge case when node has only text child") {
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.Equal) === List(XPath("test[. eq 1]")))
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.NotEqual) === List(XPath("test[. != 1]")))
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.IN) === List(XPath("test[. eq 1]")))
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.LessEqual) === List(XPath("test[. <= 1]")))
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.LessThan) === List(XPath("test[. < 1]")))
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.GreaterEqual) === List(XPath("test[. >= 1]")))
    assert(XmlToXPathDecoder.extractXPath(edgeCaseNode, ComparisonType.GreaterThan) === List(XPath("test[. > 1]")))
  }

  test("should extract xpaths from simple node") {
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.Equal) === List(XPath("test/n[. eq 1]")))
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.NotEqual) === List(XPath("test/n[. != 1]")))
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.IN) === List(XPath("test/n[. eq 1]")))
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.LessEqual) === List(XPath("test/n[. <= 1]")))
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.LessThan) === List(XPath("test/n[. < 1]")))
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.GreaterEqual) === List(XPath("test/n[. >= 1]")))
    assert(XmlToXPathDecoder.extractXPath(simpleNode, ComparisonType.GreaterThan) === List(XPath("test/n[. > 1]")))
  }

  test("should extract xpaths from complex node with many childs and attributes") {
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.Equal) ===
      List(XPath("test/n/a[(@b eq 2)][. eq 1]"), XPath("test/n/b[. eq \"a\"]")))
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.NotEqual) ===
      List(XPath("test/n/a[(@b != 2)][. != 1]"), XPath("test/n/b[. != \"a\"]")))
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.IN) ===
      List(XPath("test/n/a[(@b eq 2)][. eq 1]"), XPath("test/n/b[. eq \"a\"]")))
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.LessEqual) ===
      List(XPath("test/n/a[(@b <= 2)][. <= 1]"), XPath("test/n/b[. <= \"a\"]")))
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.LessThan) ===
      List(XPath("test/n/a[(@b < 2)][. < 1]"), XPath("test/n/b[. < \"a\"]")))
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.GreaterEqual) ===
      List(XPath("test/n/a[(@b >= 2)][. >= 1]"), XPath("test/n/b[. >= \"a\"]")))
    assert(XmlToXPathDecoder.extractXPath(complexNode, ComparisonType.GreaterThan) ===
      List(XPath("test/n/a[(@b > 2)][. > 1]"), XPath("test/n/b[. > \"a\"]")))
  }
}
