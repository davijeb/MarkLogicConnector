package org.simeont.marklogicconnector.xml

import scala.xml.Node
import scala.xml.XML
import scala.collection.JavaConversions._
import com.gigaspaces.metadata.SpaceTypeDescriptor
import com.thoughtworks.xstream.XStream
import com.gigaspaces.metadata.index.SpaceIndex
import com.gigaspaces.metadata.SpacePropertyDescriptor

object SpaceDescriptorMarshaller {

  private[this] val xstream: XStream = new XStream

  def marshallSpaceDesc(desc: SpaceTypeDescriptor): String = {
    val head = "<spacedesc superType=\"" + desc.getSuperTypeName() + "\" " +
      "type=\"" + desc.getTypeName() + "\" " +
      "typeSimple=\"" + desc.getTypeSimpleName() + "\" " +
      "id=\"" + desc.getIdPropertyName() + "\" " +
      "routing=\"" + desc.getRoutingPropertyName() + "\" " +
      "dynamicProperties=\"" + desc.supportsDynamicProperties() + "\" " +
      "optimisticLocking=\"" + desc.supportsOptimisticLocking() + "\" " +
      "autoGenerateId=\"" + desc.isAutoGenerateId() + "\" " +
      "replicable=\"" + desc.isReplicable() + "\" " +
      "storageType=\"" + desc.getStorageType() + "\" " +
      "fifoGroupingPropertyPath=\"" + desc.getFifoGroupingPropertyPath() + "\" " +
      "concreteType=\"" + desc.isConcreteType() + "\">"

    val end = "</spacedesc>"

    val fifoSupport =
      "<fifoSupport>" + xstream.toXML(desc.getFifoSupport()) + "</fifoSupport>"
    val fifoGroupingIndexesPaths =
      "<fifoGroupingIndexesPaths>" + xstream.toXML(desc.getFifoGroupingIndexesPaths()) + "</fifoGroupingIndexesPaths>"

    val documentWrapperClass =
      "<documentWrapperClass>" + xstream.toXML(desc.getDocumentWrapperClass()) + "</documentWrapperClass>"

    val fixedProperties: String =
      "<fixedProperties>" +
        (for (i <- 0 until desc.getNumOfFixedProperties())
          yield fixedPropertyToXml(desc.getFixedProperty(i))).mkString +
        "</fixedProperties>"

    val indexProperties: String =
      "<indexes>" +
        desc.getIndexes().map(pair => indexToXml(pair._2)).mkString +
        "</indexes>"

    head + fifoSupport + fifoGroupingIndexesPaths + fixedProperties + indexProperties + documentWrapperClass + end
  }

  private[this] def fixedPropertyToXml(sPropertyDescriptor: SpacePropertyDescriptor): String = {
    val head =
      "<fixProperty name=\"" + sPropertyDescriptor.getName() + "\" " +
        "storageType=\"" + sPropertyDescriptor.getStorageType() + "\" " +
        "typeName=\"" + sPropertyDescriptor.getTypeName() + "\">"
    val end =
      "</fixProperty>"
    val docSupport =
      "<documentSupport>" + xstream.toXML(sPropertyDescriptor.getDocumentSupport()) + "</documentSupport>"
    val typ =
      "<type>" + xstream.toXML(sPropertyDescriptor.getType()) + "</type>"

    head + typ + docSupport + end
  }

  def indexToXml(spaceIndex: SpaceIndex): String =
    "<index name=\"" + spaceIndex.getName() + "\" type=\"" + spaceIndex.getIndexType() + "\"/>"

  def unmarshallSpaceDesc(xmlS: String): SpaceTypeDescriptor = {
    val xml: Node = XML.loadString(xmlS)
    //TODO NOW!!!!
    null
  }
}