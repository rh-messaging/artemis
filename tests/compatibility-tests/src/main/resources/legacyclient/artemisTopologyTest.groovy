package legacyclient

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

import org.apache.activemq.artemis.api.core.TransportConfiguration
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants

import java.lang.reflect.Field

def printTopology(locator, String label) {
   try {
      Field topologyField = locator.getClass().getDeclaredField("topologyArray")
      topologyField.setAccessible(true)
      def topologyArray = topologyField.get(locator)
      if (topologyArray != null) {
         println(label + " topologyArray length: " + topologyArray.length)
         for (int t = 0; t < topologyArray.length; t++) {
            def pair = topologyArray[t]
            println("  [" + t + "] A=" + pair.getA() + " B=" + pair.getB())
         }
      } else {
         println(label + " topologyArray is null")
      }
   } catch (Exception e) {
      println(label + " Could not inspect topology: " + e.getMessage())
   }
}

Map<String, Object> params = new HashMap<String, Object>()
params.put(TransportConstants.HOST_PROP_NAME, "localhost")
params.put(TransportConstants.PORT_PROP_NAME, "61616")
def tc = new TransportConfiguration(NettyConnectorFactory.class.getName(), params)

def locator = ActiveMQClient.createServerLocatorWithHA(tc)

printTopology(locator, "Before first createSessionFactory:")

println("=== Attempting first createSessionFactory ===")

try {
   def sf = locator.createSessionFactory()
   println("first createSessionFactory succeeded")

   printTopology(locator, "After first createSessionFactory:")

   println("=== Attempting second createSessionFactory ===")
   def sf2 = locator.createSessionFactory()
   println("second createSessionFactory succeeded")

   printTopology(locator, "After second createSessionFactory:")

   sf2.close()
   sf.close()
   locator.close()
   return true
} catch (Exception e) {
   println("Exception: " + e.getClass().getName() + " - " + e.getMessage())
   e.printStackTrace()
   locator.close()
   return false
}
