/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.xml

import javax.xml.namespace.NamespaceContext
import javax.xml.stream.XMLStreamWriter

import scala.collection.mutable.Stack

class IndentingXMLStreamWriter(wr: XMLStreamWriter) extends XMLStreamWriter {
  private sealed trait WriterState
  private case object NO_NEWLINE_ON_END extends WriterState
  private case object DO_NEWLINE_ON_END extends WriterState

  private val writer = wr

  private var indentStep = "  "
  private var depth = 0

  private var state: WriterState = NO_NEWLINE_ON_END
  private val stateStack = Stack[WriterState]()

  def setIndentStep(s: String): Unit = {
    indentStep = s
  }

  private def onStartElement(): Unit = {
    stateStack.push(DO_NEWLINE_ON_END)
    state = NO_NEWLINE_ON_END
    if (depth > 0) {
      writer.writeCharacters("\n")
    }
    writeIndent()
    depth += 1
  }

  private def onEndElement(): Unit = {
    depth -= 1
    if (state == DO_NEWLINE_ON_END) {
      writer.writeCharacters("\n")
      writeIndent()
    }
    state = stateStack.pop()
  }

  private def writeIndent(): Unit = {
    writer.writeCharacters(indentStep*depth)
  }

  override def writeStartElement(localName: String): Unit = {
    onStartElement()
    writer.writeStartElement(localName)
  }

  override def writeStartElement(namespaceURI: String, localName: String): Unit = {
    onStartElement()
    writer.writeStartElement(namespaceURI, localName)
  }

  override def writeStartElement(
      prefix: String,
      localName: String,
      namespaceURI: String): Unit = {
    onStartElement()
    writer.writeStartElement(prefix, localName, namespaceURI)
  }

  override def writeEmptyElement(localName: String): Unit = {
    writer.writeEmptyElement(localName)
  }

  override def writeEmptyElement(namespaceURI: String, localName: String): Unit = {
    writer.writeEmptyElement(namespaceURI, localName)
  }

  override def writeEmptyElement(
      prefix: String,
      localName: String,
      namespaceURI: String): Unit = {
    writer.writeEmptyElement(prefix, localName, namespaceURI)
  }

  override def writeEndElement(): Unit = {
    onEndElement()
    writer.writeEndElement()
  }

  override def writeEndDocument(): Unit = {
    writer.writeEndDocument()
  }

  override def close(): Unit = {
    writer.close()
  }

  override def flush(): Unit = {
    writer.flush()
  }

  override def writeAttribute(
      prefix: String,
      namespaceURI: String,
      localName: String,
      value: String): Unit = {
    writer.writeAttribute(prefix, namespaceURI, localName, value)
  }

  override def writeAttribute(namespaceURI: String, localName: String, value: String): Unit = {
    writer.writeAttribute(namespaceURI, localName, value)
  }

  override def writeAttribute(localName: String, value: String): Unit = {
    writer.writeAttribute(localName, value)
  }

  override def writeNamespace(prefix: String, namespaceURI: String): Unit = {
    writer.writeNamespace(prefix, namespaceURI)
  }

  override def writeDefaultNamespace(namespaceURI: String): Unit = {
    writer.writeDefaultNamespace(namespaceURI)
  }

  override def writeComment(data: String): Unit = {
    writer.writeComment(data)
  }

  override def writeProcessingInstruction(target: String): Unit = {
    writer.writeProcessingInstruction(target)
  }

  override def writeProcessingInstruction(target: String, data: String): Unit = {
    writer.writeProcessingInstruction(target, data)
  }

  override def writeCData(data: String): Unit = {
    writer.writeCData(data)
  }

  override def writeDTD(dtd: String): Unit = {
    writer.writeDTD(dtd)
  }

  override def writeEntityRef(name: String): Unit = {
    writer.writeEntityRef(name)
  }

  override def writeStartDocument(): Unit = {
    writer.writeStartDocument()
  }

  override def writeStartDocument(version: String): Unit = {
    writer.writeStartDocument(version)
  }

  override def writeStartDocument(encoding: String, version: String): Unit = {
    writer.writeStartDocument(encoding, version)
  }

  override def writeCharacters(text: String): Unit = {
    state = NO_NEWLINE_ON_END
    writer.writeCharacters(text)
  }

  override def writeCharacters(text: Array[Char], start: Int, len: Int): Unit = {
    state = NO_NEWLINE_ON_END
    writer.writeCharacters(text, start, len)
  }

  override def getPrefix(uri: String): String = {
    writer.getPrefix(uri)
  }

  override def setPrefix(prefix: String, uri: String): Unit = {
    writer.setPrefix(prefix, uri)
  }

  override def setDefaultNamespace(uri: String): Unit = {
    writer.setDefaultNamespace(uri)
  }

  override def setNamespaceContext(context: NamespaceContext): Unit = {
    writer.setNamespaceContext(context)
  }

  override def getNamespaceContext: NamespaceContext = {
    writer.getNamespaceContext
  }

  override def getProperty(name: String): AnyRef = {
    writer.getProperty(name)
  }

}
