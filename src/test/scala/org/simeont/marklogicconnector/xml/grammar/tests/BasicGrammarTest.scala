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
package org.simeont.marklogicconnector.xml.grammar.tests

import java.lang.{Integer => JInteger,Double => JDouble,Short => JShort,Long => JLong,Float => JFloat,Byte => JByte}
import java.math.BigInteger
import java.math.BigDecimal
import org.scalatest.FunSuite
import com.thoughtworks.xstream.XStream
import org.simeont.marklogicconnector.xml.grammar.BasicGrammar
import org.simeont.marklogicconnector.xml.grammar.MarshalledEntry


class BasicGrammarTest extends FunSuite {

  val PROPERTY = "nothing"
  val numString = "1"
  val grammar: BasicGrammar = new BasicGrammar

  test("should marshal basic lang types"){

    val int = new JInteger(1)
    val intType = int.getClass.getName
    assert(grammar.useGrammarToMarshall(intType, int) === MarshalledEntry(intType,int.toString))

    val double = new JDouble(2.0)
    val doubleType = double.getClass.getName
    assert(grammar.useGrammarToMarshall(doubleType, double) === MarshalledEntry(doubleType,double.toString))

    val short = new JShort(numString)
    val shortType = short.getClass.getName
    assert(grammar.useGrammarToMarshall(shortType, short) === MarshalledEntry(shortType,short.toString))

    val long = new JLong(2)
    val longType = long.getClass.getName
    assert(grammar.useGrammarToMarshall(longType, long) === MarshalledEntry(longType,long.toString))

    val float = new JFloat(1)
    val floatType = float.getClass.getName
    assert(grammar.useGrammarToMarshall(floatType, float) === MarshalledEntry(floatType,float.toString))

    val byte = new JByte(numString)
    val byteType = byte.getClass.getName
    assert(grammar.useGrammarToMarshall(byteType, byte) === MarshalledEntry(byteType,byte.toString))
  }

  test("should marshal BigInteger and BigDecimal"){

    val bigInt = new BigInteger(numString)
    val intType = bigInt.getClass.getName
    assert(grammar.useGrammarToMarshall(intType, bigInt) === MarshalledEntry(intType,bigInt.toString))

    val bigDec = new BigDecimal("2.0")
    val decType = bigDec.getClass.getName
    assert(grammar.useGrammarToMarshall(decType, bigDec) === MarshalledEntry(decType,bigDec.toString))

  }

  test("should unmarshal basic lang types"){

    val int = new JInteger(1)
    val intType = int.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,intType, int.toString) === int)

    val double = new JDouble(2.0)
    val doubleType = double.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,doubleType, double.toString) === double)

    val short = new JShort(numString)
    val shortType = short.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,shortType, short.toString) === short)

    val long = new JLong(2)
    val longType = long.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,longType, long.toString) === long)

    val float = new JFloat(1)
    val floatType = float.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,floatType, float.toString) === float)

    val byte = new JByte(numString)
    val byteType = byte.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,byteType, byte.toString) === byte)
  }

  test("should unmarshal BigInteger and BigDecimal"){

    val bigInt = new BigInteger(numString)
    val intType = bigInt.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,intType, bigInt.toString) === bigInt)

    val bigDec = new BigDecimal("2.0")
    val decType = bigDec.getClass.getName
    assert(grammar.useGrammarToUnMarshall(PROPERTY,decType, bigDec.toString) === bigDec)

  }

  test("should handle string types"){

    val str = "str"
    val strType = str.getClass.getName

    assert(grammar.useGrammarToMarshall(strType, str) === MarshalledEntry(strType, str))
    assert(grammar.useGrammarToUnMarshall(PROPERTY,strType,str) === str)
  }
}
