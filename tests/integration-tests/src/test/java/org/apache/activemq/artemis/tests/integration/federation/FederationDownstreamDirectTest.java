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
package org.apache.activemq.artemis.tests.integration.federation;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.federation.FederationDownstreamConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationPolicy;
import org.apache.activemq.artemis.core.config.federation.FederationPolicySet;
import org.apache.activemq.artemis.core.config.federation.FederationQueuePolicyConfiguration;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.FederationDownstreamConnectMessage;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Before;
import org.junit.Test;

import static org.apache.activemq.artemis.core.protocol.core.impl.wireformat.FederationDownstreamConnectMessage.UPSTREAM_SUFFIX;

public class FederationDownstreamDirectTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   private static final String authorizedUser = "authorizedUser";
   private static final String authorizedPass = "authorizedPass";
   private static final String authorizedRole = "authorizedRole";

   private static final String unauthorizedUser = "unauthorizedUser";
   private static final String unauthorizedPass = "unauthorizedPass";
   private static final String unauthorizedRole = "unauthorizedRole";

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      startServer(true);
   }

   private void startServer(boolean configureAuth) throws Exception {
      Configuration config = createDefaultNettyConfig().setSecurityEnabled(true);
      if (configureAuth) {
         config.addFederationDownstreamAuthorization(authorizedRole);
      }
      server = createServer(false, config);
      server.start();

      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();

      securityManager.getConfiguration().addUser(authorizedUser, authorizedPass);
      securityManager.getConfiguration().addRole(authorizedUser, authorizedRole);

      securityManager.getConfiguration().addUser(unauthorizedUser, unauthorizedPass);
      securityManager.getConfiguration().addRole(unauthorizedUser, unauthorizedRole);
   }

   @Test
   public void testNoAuthConfigured() throws Exception {
      server.stop();
      startServer(false);
      AssertionLoggerHandler.startCapture();
      try {
         sendFederationDownstreamConnectMessage(authorizedUser, authorizedPass, false);
         assertFalse(AssertionLoggerHandler.findText("AMQ224158"));
         assertTrue(AssertionLoggerHandler.findText("AMQ224159"));
         assertFalse(AssertionLoggerHandler.findText("AMQ224160"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testUnauthenticatedDeployment() throws Exception {
      AssertionLoggerHandler.startCapture();
      try {
         sendFederationDownstreamConnectMessage(null, null, false);
         assertTrue(AssertionLoggerHandler.findText("AMQ224158"));
         assertFalse(AssertionLoggerHandler.findText("AMQ224159"));
         assertFalse(AssertionLoggerHandler.findText("AMQ224160"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testUnauthorizedDeployment() throws Exception {
      AssertionLoggerHandler.startCapture();
      try {
         sendFederationDownstreamConnectMessage(unauthorizedUser, unauthorizedPass, false);
         assertFalse(AssertionLoggerHandler.findText("AMQ224158"));
         assertTrue(AssertionLoggerHandler.findText("AMQ224159"));
         assertFalse(AssertionLoggerHandler.findText("AMQ224160"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testSuccessfulDeployment() throws Exception {
      AssertionLoggerHandler.startCapture();
      try {
         sendFederationDownstreamConnectMessage(authorizedUser, authorizedPass, true);
         assertFalse(AssertionLoggerHandler.findText("AMQ224158"));
         assertFalse(AssertionLoggerHandler.findText("AMQ224159"));
         assertTrue(AssertionLoggerHandler.findText("AMQ224160"));
         Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ224161"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   private void sendFederationDownstreamConnectMessage(String user, String password, boolean succeed) throws Exception {
      try (ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
           ClientSessionFactory factory = locator.createSessionFactory()) {
         ClientSession session = null;
         if (user != null) {
            session = factory.createSession(user, password, true, true, true, true, -1);
         }
         try {
            CoreRemotingConnection coreConn = (CoreRemotingConnection) factory.getConnection();
            Wait.assertEquals(1, server.getActiveMQServerControl()::getConnectionCount);
            Channel federationChannel = coreConn.getChannel(ChannelImpl.CHANNEL_ID.FEDERATION.id, -1);
            federationChannel.send(getFederationDownstreamConnectMessage(getName()));
            if (succeed) {
               Wait.assertTrue(() -> server.getFederationManager().get(getName() + UPSTREAM_SUFFIX) != null, 1000, 20);
            } else {
               assertFalse(Wait.waitFor(() -> server.getFederationManager().get(getName() + UPSTREAM_SUFFIX) != null, 1000, 20));
               Wait.assertEquals(0, server.getActiveMQServerControl()::getConnectionCount);
            }
         } finally {
            if (session != null) {
               session.close();
            }
         }
      }
   }

   private FederationDownstreamConnectMessage getFederationDownstreamConnectMessage(String name) {
      final String policySetName = "fake-policy-set";
      final String policyConfigName = "fake-policy-config";
      FederationDownstreamConnectMessage msg = new FederationDownstreamConnectMessage();
      msg.setName(name);

      Map<String, FederationPolicy<?>> policyMap = new HashMap<>();
      policyMap.put(policyConfigName, new FederationQueuePolicyConfiguration().setName(policyConfigName).addInclude(new FederationQueuePolicyConfiguration.Matcher().setQueueMatch("#").setAddressMatch("#")));
      policyMap.put(policySetName, new FederationPolicySet().setName(policySetName).addPolicyRef(policyConfigName));
      msg.setFederationPolicyMap(policyMap);

      FederationDownstreamConfiguration downstreamConfig = new FederationDownstreamConfiguration()
         .setName("fake")
         .addPolicyRef(policySetName);
      downstreamConfig.setUpstreamConfiguration(new TransportConfiguration(NettyConnectorFactory.class.getName(), new HashMap<>(), "fake"));
      msg.setStreamConfiguration(downstreamConfig);
      return msg;
   }
}
