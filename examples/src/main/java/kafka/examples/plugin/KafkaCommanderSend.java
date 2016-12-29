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
package kafka.examples.plugin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by yestin on 2016/12/20.
 */
public class KafkaCommanderSend {
    public static void main(String[] args) throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress("localhost",9093);

        SocketChannel socketChannel = SocketChannel.open();

//        Selector selector = Selector.open();
//
//        socketChannel.configureBlocking(false);
//        socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

        socketChannel.connect(socketAddress);

        String command = "startReceive";

        socketChannel.write(ByteBuffer.wrap(command.getBytes()));

        socketChannel.socket().shutdownOutput();

        System.out.println("send: " + command);



        ByteBuffer returnBuffer = ByteBuffer.allocate(1024);


        int size = socketChannel.read(returnBuffer);
        byte[] bytes = new byte[size];
        returnBuffer.flip();
        returnBuffer.get(bytes, 0, size);
        System.out.println("receive: " + new String(bytes));
        socketChannel.close();

//        while(selector.select() > 0) {
//            Set<SelectionKey> readyKeys = selector.selectedKeys();
//            Iterator it = readyKeys.iterator();
//            while (it.hasNext()) {
//                SelectionKey key = null;
//                key = (SelectionKey) it.next();
//                it.remove();
//                if (key.isReadable()) {
//                } if (key.isWritable()) {
//                }
//            }
//        }
    }
}
