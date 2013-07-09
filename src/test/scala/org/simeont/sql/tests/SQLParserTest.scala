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
package org.simeont.sql.tests

import org.scalatest.FunSuite
import org.simeont.sql.parser.GsSqlParser
import org.simeont.sql.parser.data._

class SQLParserTest extends FunSuite {

  test("simple eq/not eq sql expression") {
    assert(GsSqlParser("a = ?") === Eq("a"))
    assert(GsSqlParser("a eq ?") === Eq("a"))
    assert(GsSqlParser("a != ?") === NotEq("a"))
  }

  test("simple IN sql expression") {
    assert(GsSqlParser("a IN (?)") === In("a", 1))
    assert(GsSqlParser("a.b IN (?, ?)") === In("a.b", 2))
  }

  test("simple comperisons sql expression") {
    assert(GsSqlParser("a < ?") === Less("a"))
    assert(GsSqlParser("a.b > ?") === Greater("a.b"))
    assert(GsSqlParser("aca <= ?") === LessEq("aca"))
    assert(GsSqlParser("bba >= ?") === GreaterEq("bba"))
  }

  test("and/or sql expression") {
    assert(GsSqlParser("a < ? AND b = ?") === And(List(Less("a"), Eq("b"))))
    assert(GsSqlParser("b = ? OR k >= ?") === Or(List(Eq("b"), GreaterEq("k"))))
  }

  test("complex sql expression") {
    assert(GsSqlParser("a < ? AND b = ? OR k >= ?") === Or(List(And(List(Less("a"), Eq("b"))), GreaterEq("k"))))
    assert(GsSqlParser("a > ? AND a < ? AND b = ? OR k >= ?") === Or(List(And(List(Greater("a"), Less("a"), Eq("b"))), GreaterEq("k"))))
    assert(GsSqlParser("a > ? AND a < ? OR b = ? AND k >= ?") === Or(List(And(List(Greater("a"), Less("a"))), And(List(Eq("b"), GreaterEq("k"))))))
    assert(GsSqlParser("a > ? OR a < ? OR b = ? OR k >= ?") === Or(List(Greater("a"), Less("a"), Eq("b"), GreaterEq("k"))))
    assert(GsSqlParser("a < ? AND b = ? AND k >= ?") === And(List(Less("a"), Eq("b"), GreaterEq("k"))))
  }
}
