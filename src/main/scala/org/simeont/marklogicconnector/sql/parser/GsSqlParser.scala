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

import scala.util.parsing.combinator.RegexParsers
import org.simeont.marklogicconnector.sql.parser.data._

object GsSqlParser extends RegexParsers {
  def sqlExp: Parser[Exp] = """([^\s(])+\s(=|(!=)|(>=)|(IN)|(<=)|>|<|(eq))\s((\([?,\s]*?\))|\?)""".r ^^ {
    s => s match {
      case oper : String if oper.contains(" IN ") => { val words = s.split(" IN "); In(words(0), words(1).count(_=='?'))}
      case oper : String if oper.contains(" = ") || oper.contains(" eq ") => Eq(s.takeWhile(_!=' '))
      case oper : String if oper.contains(" < ")  => Less(s.takeWhile(_!=' '))
      case oper : String if oper.contains(" <= ")  => LessEq(s.takeWhile(_!=' '))
      case oper : String if oper.contains(" >= ")  => GreaterEq(s.takeWhile(_!=' '))
      case oper : String if oper.contains(" > ")  => Greater(s.takeWhile(_!=' '))
      case oper : String if oper.contains(" != ")  => NotEq(s.takeWhile(_!=' '))
      case oper : String if (oper.contains(" rlike ") ||  oper.contains(" like ") )  => Like(s.takeWhile(_!=' '))
      case oper : String if oper.contains(" not like ")  => NotLike(s.takeWhile(_!=' '))
    }
  }

  def combination: Parser[Exp] =  sqlExp |  andCombination

  def andCombination: Parser[Exp] = combination ~ rep("AND" ~ combination) ^^ {
    case number ~ list => list.foldLeft(number) {
      case (x, "AND" ~ y) => And(List(x, y))
    }
  }

  def expr: Parser[Exp] = andCombination ~ rep( "OR" ~ andCombination) ^^ {
    case number ~ list => list.foldLeft(number) {
      case (x, "OR" ~ y) => Or(List(x, y))
    }
  }

  def apply(input: String): Exp = parseAll(expr, input) match {
    case Success(result, _) => result match {
      case oper : ConditionOperation => {oper.normilize; oper}
      case any : Exp => any
    }
    case failure: NoSuccess => scala.sys.error(failure.msg)
  }
}
