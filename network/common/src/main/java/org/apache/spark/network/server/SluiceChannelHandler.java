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

package org.apache.spark.network.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.SluiceClient;
import org.apache.spark.network.client.SluiceResponseHandler;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.request.RequestMessage;
import org.apache.spark.network.protocol.response.ResponseMessage;
import org.apache.spark.network.util.NettyUtils;

/**
 * A handler which is used for delegating requests to the
 * {@link org.apache.spark.network.server.SluiceRequestHandler} and responses to the
 * {@link org.apache.spark.network.client.SluiceResponseHandler}.
 *
 * All channels created in Sluice are bidirectional. When the Client initiates a Netty Channel
 * with a RequestMessage (which gets handled by the Server's RequestHandler), the Server will
 * produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server also
 * gets a handle on the same Channel, so it may then begin to send RequestMessages to the Client.
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 */
public class SluiceChannelHandler extends SimpleChannelInboundHandler<Message> {
  private final Logger logger = LoggerFactory.getLogger(SluiceChannelHandler.class);

  private final SluiceClient client;
  private final SluiceResponseHandler responseHandler;
  private final SluiceRequestHandler requestHandler;

  public SluiceChannelHandler(
      SluiceClient client,
      SluiceResponseHandler responseHandler,
      SluiceRequestHandler requestHandler) {
    this.client = client;
    this.responseHandler = responseHandler;
    this.requestHandler = requestHandler;
  }

  public SluiceClient getClient() {
    return client;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Exception in connection from " + NettyUtils.getRemoteAddress(ctx.channel()),
      cause);
    requestHandler.exceptionCaught(cause);
    responseHandler.exceptionCaught(cause);
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    requestHandler.channelUnregistered();
    responseHandler.channelUnregistered();
    super.channelUnregistered(ctx);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Message request) {
    if (request instanceof RequestMessage) {
      requestHandler.handle((RequestMessage) request);
    } else {
      responseHandler.handle((ResponseMessage) request);
    }
  }
}
