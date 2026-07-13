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

import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.utils.AbstractMapPersister;

/**
 * Core connect handshake sent after the transport is established.
 * <p>
 * Replaces the former {@code CheckFailoverMessage} on the same wire packet ID
 * ({@link PacketImpl#CONNECT}, {@code -4}). The node id field is unchanged;
 * optional client version and connection credentials may follow as extension
 * fields for backward-compatible decoding.
 */
public class ConnectMessage extends PacketImpl {

   static final short FIELD_CLIENT_VERSION = 1;
   static final short FIELD_USER = 2;
   static final short FIELD_PASSWORD = 3;

   // ConnectMapSerializer is stateless and safe to be used from multiple threads.
   // The state needed is on ConnectMessage and ActiveMQBuffer only
   private static final ConnectMapSerializer mapSerializer = new ConnectMapSerializer();

   private static class ConnectMapSerializer extends AbstractMapPersister<ConnectMessage> {
      public void encode(ActiveMQBuffer buffer, ConnectMessage message) {
         int headerPosition = buffer.writerIndex();
         writeHeader(buffer, 0, 0); // to reserve the header, will be rewritten at the end

         int fields = 0;

         // Encode other fields as an extension ensure backward compatibility.
         if (message.clientVersion > 0) {
            writeInteger(buffer, FIELD_CLIENT_VERSION, message.clientVersion);
            fields++;
         }

         if (message.user != null) {
            writeString(buffer, FIELD_USER, message.user);
            fields++;
         }

         if (message.password != null) {
            writeString(buffer, FIELD_PASSWORD, message.password);
            fields++;
         }

         int endPosition = buffer.writerIndex();
         buffer.writerIndex(headerPosition);
         writeHeader(buffer, endPosition - headerPosition, fields);
         buffer.writerIndex(endPosition);
      }

      @Override
      protected int getMaxAllowedElements() {
         return 10;
      }

      @Override
      protected void onMapReadInteger(short key, int value, ConnectMessage decodingObject) {
         switch (key) {
            case FIELD_CLIENT_VERSION -> {
               decodingObject.clientVersion = value;
            }
         }
      }

      @Override
      protected void onMapReadByte(short key, byte value, ConnectMessage decodingObject) {
      }

      @Override
      protected void onMapReadBoolean(short key, boolean value, ConnectMessage decodingObject) {

      }

      @Override
      protected void onMapReadLong(short key, long value, ConnectMessage decodingObject) {

      }

      @Override
      protected void onMapReadString(short key, String value, ConnectMessage decodingObject) {
         switch (key) {
            case FIELD_USER -> {
               decodingObject.user = value;
            }
            case FIELD_PASSWORD -> {
               decodingObject.password = value;
            }
         }
      }

      @Override
      protected void onMapReadByteArray(short key, ActiveMQBuffer slice, ConnectMessage decodingObject) {

      }
   }

   /**
    * Additional field type bytes may be appended after nodeID.
    * Decoders skip unknown fields to preserve forward compatibility.
    */

   private String nodeID;

   private int clientVersion;

   private String user;

   private String password;

   public ConnectMessage(final String nodeID,
                         final int clientVersion,
                         final String username,
                         final String password) {
      super(CONNECT);
      this.nodeID = nodeID;
      this.clientVersion = clientVersion;
      this.user = username;
      this.password = password;
   }

   public ConnectMessage() {
      super(CONNECT);
   }

   @Override
   public void encodeRest(ActiveMQBuffer buffer) {
      buffer.writeNullableString(nodeID);

      mapSerializer.encode(buffer, this);

   }

   @Override
   public void decodeRest(ActiveMQBuffer buffer) {
      nodeID = buffer.readNullableString();

      if (buffer.readableBytes() > 0) {
         mapSerializer.decode(buffer, this);
      }
   }

   @Override
   protected String getPacketString() {
      StringBuilder sb = new StringBuilder(super.getPacketString());
      sb.append(", nodeID=").append(nodeID);
      sb.append(", clientVersion=").append(clientVersion);
      if (user != null) {
         sb.append(", username=").append(user);
      }
      return sb.toString();
   }

   public String getNodeID() {
      return nodeID;
   }

   public int getClientVersion() {
      return clientVersion;
   }

   public String getUser() {
      return user;
   }

   public String getPassword() {
      return password;
   }

   @Override
   public int hashCode() {
      return super.hashCode() + Objects.hash(nodeID, clientVersion, user, password);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof ConnectMessage other)) {
         return false;
      }

      return Objects.equals(nodeID, other.nodeID)
         && Objects.equals(clientVersion, other.clientVersion)
         && Objects.equals(user, other.user)
         && Objects.equals(password, other.password);
   }
}
