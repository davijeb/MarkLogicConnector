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
package org.simeont.marklogicconnector.sql.parser.data

/**
 * The super type of all expressions produced by [[org.simeont.marklogicconnector.sql.parser.GsSqlParser]]
 * The GigaSpaces SQL is has a specific representation of the SQL, the parameters are transfered separate from the
 * query and at there place a question mark is placed.
 */
sealed abstract class Exp

/**
 * The super type of all condition operations - or/and
 */
sealed abstract class ConditionOperation extends Exp {
  def combined : List[Exp]
  def normilize : Unit
}

/**
 * AND dictates that all expressions stored should be combined using 'and' operation
 */
case class And(var operands : List[Exp])extends ConditionOperation{
	override def combined : List[Exp] = operands.flatMap( x=> x match {
	  case o : And => o.combined
	  case op : ConditionOperation => {op.normilize; List(op)}
	  case any : Exp => List(any)
	})

	override def normilize : Unit = operands = combined
}

/**
 * OR dictates that all expressions stored should be combined using 'or' operation
 */
case class Or(var operands : List[Exp]) extends ConditionOperation{
  override def combined : List[Exp] = operands.flatMap( x=> x match {
	  case o : Or => o.combined
	  case op : ConditionOperation => {op.normilize; List(op)}
	  case any : Exp => List(any)
	})

	override def normilize : Unit = operands = combined
}

/**
 * An equals operation needs to be performed for this property
 */
case class Eq( property : String) extends Exp

/**
 * An In operation needs to be performed for this property, count dictates how many of the parameters are to be
 * included in the IN check.
 */
case class In(property : String, count : Int) extends Exp

/**
 * A less then operation needs to be performed for this property
 */
case class Less( property : String) extends Exp

/**
 * A less then or equal operation needs to be performed for this property
 */
case class LessEq( property : String) extends Exp

/**
 * A greater then operation needs to be performed for this property
 */
case class Greater( property : String) extends Exp

/**
 * A greater then or equal operation needs to be performed for this property
 */
case class GreaterEq( property : String) extends Exp

/**
 * A not equals operation needs to be performed for this property
 */
case class NotEq( property : String) extends Exp

/**
 * A like operation needs to be performed for this property
 */
case class Like( property : String) extends Exp

/**
 *  A not like operation needs to be performed for this property
 */
case class NotLike( property : String) extends Exp
