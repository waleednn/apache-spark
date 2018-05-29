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

package org.apache.spark.ui

import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.config._

/**
 * A servlet filter that implements HTTP security features. The following actions are taken
 * for every request:
 *
 * - perform access control of authenticated requests.
 * - check request data for disallowed content (e.g. things that could be used to create XSS
 *   attacks).
 * - set response headers to prevent certain kinds of attacks.
 */
private class HttpSecurityFilter(
    conf: SparkConf,
    securityMgr: SecurityManager) extends Filter {

  private val NEWLINE_AND_SINGLE_QUOTE_REGEX = raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r

  override def destroy(): Unit = { }

  override def init(config: FilterConfig): Unit = { }

  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]
    val hres = res.asInstanceOf[HttpServletResponse]
    hres.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")

    if (!securityMgr.checkUIViewPermissions(hreq.getRemoteUser())) {
      hres.sendError(HttpServletResponse.SC_FORBIDDEN,
        "User is not authorized to access this page.")
      return
    }

    // Check if any disallowed content is in the incoming headers or parameters. This filters
    // out content that could be used for XSS attacks from even making it to the UI handlers.
    try {
      hreq.getHeaderNames().asScala.foreach { k =>
        require(isSafe(k), "Request header name contains disallowed content.")
        hreq.getHeaders(k).asScala.foreach { v =>
          require(isSafe(v), s"Header value for $k contains disallowed content.")
        }
      }

      hreq.getParameterMap().asScala.foreach { case (k, values) =>
        require(isSafe(k), "Parameter name contains disallowed content.")
        values.foreach { v =>
          require(isSafe(v), s"Parameter value for $k contains disallowed content.")
        }
      }
    } catch {
      case e: IllegalArgumentException =>
        // Do not allow the request to go further if any problem is detected.
        hres.setContentType("text/plain;charset=utf-8")
        hres.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
        return
    }

    // SPARK-10589 avoid frame-related click-jacking vulnerability, using X-Frame-Options
    // (see http://tools.ietf.org/html/rfc7034). By default allow framing only from the
    // same origin, but allow framing for a specific named URI.
    // Example: spark.ui.allowFramingFrom = https://example.com/
    val xFrameOptionsValue = conf.getOption("spark.ui.allowFramingFrom")
      .map { uri => s"ALLOW-FROM $uri" }
      .getOrElse("SAMEORIGIN")

    hres.setHeader("X-Frame-Options", xFrameOptionsValue)
    hres.setHeader("X-XSS-Protection", conf.get(UI_X_XSS_PROTECTION))
    if (conf.get(UI_X_CONTENT_TYPE_OPTIONS)) {
      hres.setHeader("X-Content-Type-Options", "nosniff")
    }
    if (hreq.getScheme() == "https") {
      conf.get(UI_STRICT_TRANSPORT_SECURITY).foreach(
        hres.setHeader("Strict-Transport-Security", _))
    }

    chain.doFilter(req, res)
  }

  private def isSafe(str: String): Boolean = stripXSS(str) == str

  private def stripXSS(str: String): String = {
    if (str != null) {
      // Remove new lines and single quotes, followed by escaping HTML version 4.0
      StringEscapeUtils.escapeHtml4(NEWLINE_AND_SINGLE_QUOTE_REGEX.replaceAllIn(str, ""))
    } else {
      null
    }
  }

}
