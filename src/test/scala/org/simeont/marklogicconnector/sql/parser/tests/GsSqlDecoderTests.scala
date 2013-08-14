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
import org.simeont.marklogicconnector.xml.BasicMarshaller
import org.simeont.marklogicconnector.sql.parser.LessEq

class GsSqlDecoderTests extends FunSuite {
  val marshaller = new BasicMarshaller
  val decoded = new GsSqlDecoder(marshaller)
  val data = List("b", "c", "d")

  test("should decode simple sql") {

    assert(decoded.decodeSQL(Less("a"), data) === "(./a[. < \"b\"])")
    assert(decoded.decodeSQL(LessEq("a"), data) === "(./a[. <= \"b\"])")
    assert(decoded.decodeSQL(Greater("a"), data) === "(./a[. > \"b\"])")
    assert(decoded.decodeSQL(GreaterEq("a"), data) === "(./a[. >= \"b\"])")
    assert(decoded.decodeSQL(Eq("a"), data) === "(./a[. eq \"b\"])")
    assert(decoded.decodeSQL(NotEq("a"), data) === "(./a[. != \"b\"])")
  }

  test("should decode In sql"){

     assert(decoded.decodeSQL(In("a",2), data.slice(0,2)) === "( (./a[. eq \"b\"]) or (./a[. eq \"c\"]) )")
  }

  test("should decode complex sql") {
    var exp: Exp = And(List(Less("a"), Greater("a")))

    assert(decoded.decodeSQL(exp, data) === "( (./a[. < \"b\"]) and (./a[. > \"c\"]) )")

    exp = Or(List(And(List(Less("a"), Greater("a"))), Greater("a")))

    assert(decoded.decodeSQL(exp, data) === "( ( (./a[. < \"b\"]) and (./a[. > \"c\"]) ) or (./a[. > \"d\"]) )")
  }

}
