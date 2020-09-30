package io.stargate.web.swagger;

import io.stargate.web.RestApiActivator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/swagger-static")
@Produces({MediaType.TEXT_HTML, "text/css", "image/png"})
public class SwaggerStaticResource {

  private static final Logger logger = LoggerFactory.getLogger(SwaggerStaticResource.class);

  private static Pattern fileExtensionPattern =
      Pattern.compile("([^\\s]+(\\.(?i)(css|png|js|map))$)");

  /**
   * Due to how class loading works, we can't use the standard {@code
   * io.dropwizard.servlets.assets.AssetServlet} so instead we have to serve up our own static
   * resources
   *
   * @param fileName The name of the file that should be returned from the swagger-static directory
   * @return An {@code javax.ws.rs.core.Response} containing an {@code java.io.InputStream} of the
   *     file requested
   */
  @GET
  @Path("/{fileName}")
  public Response get(@PathParam("fileName") final String fileName) {
    // Immediately return a 404 if an unsupported extension is used
    Matcher matcher = fileExtensionPattern.matcher(fileName);
    if (!matcher.matches()) {
      return Response.status(Status.NOT_FOUND).build();
    }

    InputStream is;
    String type = MediaType.TEXT_HTML;
    try {
      Bundle bundle = FrameworkUtil.getBundle(RestApiActivator.class);
      URL entry = bundle.getEntry("/swagger-static/" + fileName);
      is = entry.openConnection().getInputStream();

      if (fileName.endsWith(".css")) {
        type = "text/css";
      } else if (fileName.endsWith(".png")) {
        type = "image/png";
      }
    } catch (FileNotFoundException e) {
      return Response.status(Status.NOT_FOUND).build();
    } catch (IOException ioe) {
      logger.error("Error when executing request", ioe);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
    return Response.ok(is).type(type).build();
  }
}