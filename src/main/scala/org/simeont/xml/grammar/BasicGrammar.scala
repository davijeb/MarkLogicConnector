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
package org.simeont.xml.grammar

import java.lang.{Integer => JInteger,Double => JDouble,Short => JShort,Long => JLong,Float => JFloat,Byte => JByte}
import com.thoughtworks.xstream.XStream
import java.math.BigInteger
import java.math.BigDecimal

/**
 * Helper object wraps around functions to translate different types to and from XML.
 *
 * @author Tomo Simeonov
 */
case class GrammarSimpleHolder(toXML: AnyRef => String, fromXML: String => AnyRef)

/**
 * Basic implementation of the grammar using XStream as marshaller for unknown objects and some hard coded translations.
 *
 * @author Tomo Simeonov
 */
class BasicGrammar extends Grammar {

  val xstream: XStream = new XStream

  val UNKNOWN_TYPE: String = "object"

  override def useGrammarToMarshall(propertyName: String, obj: AnyRef): MarshalledEntry = {
    val key = obj.getClass.getName
    grammarMap.get(key) match {
      case Some(holder) => MarshalledEntry(key, holder.toXML(obj))
      case None => MarshalledEntry(UNKNOWN_TYPE, grammarMap.get(UNKNOWN_TYPE).get toXML obj)
    }
  }

  override def useGrammarToUnMarshall(propertyName: String, typ: String, body: String): AnyRef = {
    grammarMap.get(typ).get fromXML body
  }

  var grammarMap: Map[String, GrammarSimpleHolder] = Map[String, GrammarSimpleHolder](
    "java.lang.Integer" -> GrammarSimpleHolder({ int: Any => int.toString }, { str: String => JInteger.valueOf(str) }),
    "java.lang.Short" -> GrammarSimpleHolder({ short: Any => short.toString }, { str: String => JShort.valueOf(str) }),
    "java.lang.Byte" -> GrammarSimpleHolder({ byt: Any => byt.toString }, { str: String => JByte.valueOf(str) }),
    "java.lang.Float" -> GrammarSimpleHolder({ float: Any => float.toString }, { str: String => JFloat.valueOf(str) }),
    "java.lang.Long" -> GrammarSimpleHolder({ long: Any => long.toString }, { str: String => JLong.valueOf(str) }),
    "java.lang.Double" -> GrammarSimpleHolder({ double: Any => double.toString }, { str: String => JDouble.valueOf(str) }),
    "java.math.BigInteger" -> GrammarSimpleHolder({ long: Any => long.toString }, { str: String => new BigInteger(str) }),
    "java.math.BigDecimal" -> GrammarSimpleHolder({ double: Any => double.toString }, { str: String => new BigDecimal(str) }),
    "java.lang.String" -> GrammarSimpleHolder({ str: Any => str.toString }, { str: String => str }),
    UNKNOWN_TYPE -> GrammarSimpleHolder({ o: Any => xstream.toXML(o) }, { str: String => xstream.fromXML(str) }))

}
