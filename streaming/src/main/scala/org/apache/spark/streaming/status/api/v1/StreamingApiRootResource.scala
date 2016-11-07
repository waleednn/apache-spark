package org.apache.spark.streaming.status.api.v1

import org.apache.spark.status.api.v1.UIRoot
import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

import com.sun.jersey.spi.container.servlet.ServletContainer

import javax.servlet.ServletContext
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.Context
import org.apache.spark.streaming.ui.StreamingJobProgressListener


@Path("/v1")
private[v1] class StreamingApiRootResource extends UIRootFromServletContext{

  @Path("streaminginfo")
  def getStreamingInfo(): StreamingInfoResource = {
    new StreamingInfoResource(uiRoot,listener)
  }
  
}

private[spark] object StreamingApiRootResource {

  def getServletHandler(uiRoot: UIRoot, listener:StreamingJobProgressListener): ServletContextHandler = {

    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/streamingapi")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
      "com.sun.jersey.api.core.PackagesResourceConfig")
    holder.setInitParameter("com.sun.jersey.config.property.packages",
      "org.apache.spark.streaming.status.api.v1")
    //holder.setInitParameter(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS,
    //  classOf[SecurityFilter].getCanonicalName)
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    UIRootFromServletContext.setListener(jerseyContext, listener)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}

private[v1] object UIRootFromServletContext {

  private val attribute = getClass.getCanonicalName

  def setListener(contextHandler:ContextHandler, listener: StreamingJobProgressListener):Unit={
   contextHandler.setAttribute(attribute+"_listener", listener) 
  }
  
  def getListener(context:ServletContext):StreamingJobProgressListener={
    context.getAttribute(attribute+"_listener").asInstanceOf[StreamingJobProgressListener]
  }
  
  def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit = {
    contextHandler.setAttribute(attribute, uiRoot)
  }

  def getUiRoot(context: ServletContext): UIRoot = {
    context.getAttribute(attribute).asInstanceOf[UIRoot]
  }
}

private[v1] trait UIRootFromServletContext {
  @Context
  var servletContext: ServletContext = _

  def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)
  def listener: StreamingJobProgressListener = UIRootFromServletContext.getListener(servletContext)
}
