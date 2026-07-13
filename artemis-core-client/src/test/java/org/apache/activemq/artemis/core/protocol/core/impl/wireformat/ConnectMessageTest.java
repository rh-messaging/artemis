/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.junit.jupiter.api.Test;

public class ConnectMessageTest {

   static final int TEST_VERSION = 123;

   @Test
   public void testRoundTripWithoutCredentials() {
      ConnectMessage encoded =
         new ConnectMessage("node-1", TEST_VERSION, null, null);

      ConnectMessage decoded = roundTrip(encoded);

      assertEquals("node-1", decoded.getNodeID());
      assertEquals(TEST_VERSION, decoded.getClientVersion());
      assertNull(decoded.getUser());
      assertNull(decoded.getPassword());
   }

   @Test
   public void testRoundTripWithCredentials() {
      ConnectMessage encoded =
         new ConnectMessage("node-1", TEST_VERSION, "cluster-user", "cluster-pass");

      ConnectMessage decoded = roundTrip(encoded);

      assertEquals("node-1", decoded.getNodeID());
      assertEquals(TEST_VERSION, decoded.getClientVersion());
      assertEquals("cluster-user", decoded.getUser());
      assertEquals("cluster-pass", decoded.getPassword());
   }

   @Test
   public void testRoundTripWithNullPassword() {
      ConnectMessage encoded =
         new ConnectMessage("node-1", TEST_VERSION, "cluster-user", null);

      ConnectMessage decoded = roundTrip(encoded);

      assertEquals("cluster-user", decoded.getUser());
      assertNull(decoded.getPassword());
   }

   @Test
   public void testRoundTripWithEmptyPassword() {
      ConnectMessage encoded =
         new ConnectMessage("node-1", TEST_VERSION, "cluster-user", "");

      ConnectMessage decoded = roundTrip(encoded);

      assertEquals("cluster-user", decoded.getUser());
      assertEquals("", decoded.getPassword());
   }

   @Test
   public void testDecodeIgnoresUnknownFutureSections() {
      ConnectMessage encoded =
         new ConnectMessage("node-1", TEST_VERSION, "cluster-user", "cluster-pass");
      ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(256);
      encoded.encodeRest(buffer);
      buffer.writeByte((byte) 99);
      buffer.writeInt(42);

      ConnectMessage decoded = new ConnectMessage();
      decoded.decodeRest(buffer);

      assertEquals("node-1", decoded.getNodeID());
      assertEquals(TEST_VERSION, decoded.getClientVersion());
      assertEquals("cluster-user", decoded.getUser());
      assertEquals("cluster-pass", decoded.getPassword());
   }

   private static ConnectMessage roundTrip(ConnectMessage encoded) {
      ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(256);
      encoded.encodeRest(buffer);

      ConnectMessage decoded = new ConnectMessage();
      decoded.decodeRest(buffer);
      return decoded;
   }
}
