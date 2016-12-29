/**
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

package kafka.plugin

import java.net.{InetSocketAddress, SocketException}
import java.nio.ByteBuffer
import java.nio.channels._
import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import kafka.common.KafkaException
import kafka.network._
import kafka.utils.{Logging, Utils}

/**
  * receive command
  */
class KafkaCommander extends Logging{

  this.logIdent = "[Socket Server for KafkaCommander], "

  var serverChannel:ServerSocketChannel = null
  var commanderAcceptor: CommanderAcceptor = null
  var commandProcessor: CommandProcessor = null
  def startup(host: String, port: Int): Unit = {
    val connectionQuats = new ConnectionQuotas(10, Map.empty[String,Int])
    commandProcessor = new CommandProcessor(connectionQuats)
    Utils.newThread("kafka-command-processor", commandProcessor, false).start()

    commanderAcceptor = new CommanderAcceptor(host, port, commandProcessor, 1024, 1024, connectionQuats)

    Utils.newThread("kafka-command-acceptor", commanderAcceptor, false).start()

    commanderAcceptor.awaitStartup()
    info("kafka commander started")
  }

  def shutdown(): Unit ={
    info("kafka commander shutting down")
    commandProcessor.shutdown()
    commanderAcceptor.shutdown()
    info("kafka commander shutdown completed")
  }
}

class StringSend(content: String) extends Send {

  override def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    channel.write(ByteBuffer.wrap(content.getBytes()))
    complete = true
    content.getBytes.size
  }


  var complete: Boolean = false
}

class StringReceive extends Receive{

  var actualByteBuffer: ByteBuffer = null

  override def buffer: ByteBuffer = {
    expectComplete()
    actualByteBuffer
  }

  def content: String = {
    expectComplete()
    new String(actualByteBuffer.array())
  }

  override def readFrom(channel: ReadableByteChannel): Int = {
    expectIncomplete()
    val maxByteBuffer = ByteBuffer.allocate(1024);
    val read = channel.read(maxByteBuffer)
    if (read < 0) {
      throw new InvalidRequestException("size < 0")
    }
    trace(s"read : $read")
    actualByteBuffer = ByteBuffer.allocate(read)
    actualByteBuffer.put(maxByteBuffer.array(), 0, read)
    trace("read finished")
    complete = true
    read
  }

  var complete: Boolean = false
}

class CommandProcessor (connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas){

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val lruConnections = new util.LinkedHashMap[SelectionKey, Long]

  def accept(socketChannel: SocketChannel): Unit = {
    newConnections.add(socketChannel)
    wakeup()
  }

  override def run(): Unit = {
   startupComplete()
    while (isRunning) {
      configureNewConnections()
      val ready = selector.select(300)
      if (ready > 0){
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while (iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next()
            iter.remove()
            if (key.isReadable)
              read(key)
            if (key.isWritable) {
              write(key)
            }
          } catch {
            case e: Throwable => {
              error("Error while accepting connection", e)
              close(key)
            }
          }
        }
      }
    }
    debug("Closing processor.")
    closeAll()
    swallowError(selector.close())
    shutdownComplete()
  }

  def write(key: SelectionKey): Unit = {
    val socketChannel = key.channel().asInstanceOf[SocketChannel]
    key.attachment().asInstanceOf[StringSend].writeTo(socketChannel)
    key.interestOps(key.interestOps & (~SelectionKey.OP_WRITE))
  }

  def read(key: SelectionKey): Unit = {
    lruConnections.put(key, System.currentTimeMillis())
    val socketChannel = key.channel().asInstanceOf[SocketChannel]
    var receive = key.attachment().asInstanceOf[Receive]
    if (key.attachment() == null) {
      receive = new StringReceive
      key.attach(receive)
    }
    val read = receive.readFrom(socketChannel)
    val address = socketChannel.socket().getRemoteSocketAddress
    debug(read + " bytes read from " + address)
    if (read < 0) {
      close(key)
    } else if (receive.complete) {
      key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
      process(key)
    } else {
      // no else.
      close(key)
    }
  }

  def process(key: SelectionKey): Unit = {
    val receive = key.attachment().asInstanceOf[StringReceive]
    receive.content match {
      case "stopReceive" =>
        info("stop receive : origin state : " + KafkaCommandState.canProduce)
        KafkaCommandState.canProduce = false
        key.attach(new StringSend("ok"))
        key.interestOps(key.interestOps | SelectionKey.OP_WRITE)
      case "startReceive" =>
        info("start receive : origin state : " + KafkaCommandState.canProduce)
        KafkaCommandState.canProduce = true
        key.attach(new StringSend("ok"))
        key.interestOps(key.interestOps | SelectionKey.OP_WRITE)
      case "describe" =>
        info(s"describe server state : $KafkaCommandState")
        key.attach(new StringSend(KafkaCommandState.toString))
        key.interestOps(key.interestOps | SelectionKey.OP_WRITE)
    }
  }

  /**
    * Register any new connections that have been queued up
    */
  private def configureNewConnections() {
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      debug("Command Processor listening to new connection from " + channel.socket.getRemoteSocketAddress)
      channel.register(selector, SelectionKey.OP_READ)
    }
  }
}

class CommanderAcceptor(val host:String,
                        val port: Int,
                        val commandProcessor: CommandProcessor,
                        val sendBufferSize: Int,
                        val recvBufferSize: Int,
                        connectionQuotas: ConnectionQuotas)
  extends AbstractServerThread(connectionQuotas){

  val serverChannel = openServerSocket(host, port)

  override def run(): Unit = {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT)
    startupComplete()
    while(isRunning) {
      val ready = selector.select(500)
      if (ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while (iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next()
            iter.remove()
            if (key.isAcceptable)
              accept(key, commandProcessor)
            else
              throw new IllegalStateException("Unrecognized key state for acceptor thread.")
          } catch {
            case e: Throwable => error("Error while accepting connection", e)
          }
        }
      }
    }
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(selector.close())
    shutdownComplete()
  }

  def accept(key: SelectionKey, processor: CommandProcessor): Unit ={
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
        .format(socketChannel.socket.getInetAddress, socketChannel.socket.getLocalSocketAddress,
          socketChannel.socket.getSendBufferSize, sendBufferSize,
          socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(1024)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, port))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
    }
    serverChannel
  }
}

object KafkaCommandState {

  var canProduce = true

  override def toString: String = {
    s"canProduce=$canProduce"
  }

}
