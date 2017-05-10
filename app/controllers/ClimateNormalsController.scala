/*
    MET-API

    Copyright (C) 2014 met.no
    Contact information:
    Norwegian Meteorological Institute
    Box 43 Blindern
    0313 OSLO
    NORWAY
    E-mail: met-api@met.no

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
    MA 02110-1301, USA
*/

package controllers

import play.api._
import play.api.mvc._
import play.api.http.Status._
import com.github.nscala_time.time.Imports._
import javax.inject.Inject
import io.swagger.annotations._
import scala.language.postfixOps
import util._
import no.met.data._
import models.ClimateNormal
import services.climatenormals._


@Api(value = "climatenormals")
class ClimateNormalsController @Inject()(climateNormalsAccess: ClimateNormalsAccess) extends Controller {

  @ApiOperation(
    value = "Get climate normals.",
    notes = "Get climate normals. To be expanded.",
    response = classOf[models.ClimateNormalsResponse],
    httpMethod = "GET")
  @ApiResponses(Array(
    // scalastyle:off magic.number
    new ApiResponse(code = 400, message = "Invalid parameter value or malformed request."),
    new ApiResponse(code = 401, message = "Unauthorized client ID."),
    new ApiResponse(code = 404, message = "No data was found for this combination of query parameters."),
    new ApiResponse(code = 500, message = "Internal server error."))
    // scalastyle:on magic.number
  )
  def getClimateNormals( // scalastyle:ignore public.methods.have.type
    // scalastyle:off line.size.limit
    @ApiParam(value = "The sources to get climate normals for as a comma-separated list. Each source should be of the form SN&lt;number&gt;.", required = true)
    sources: String,
    @ApiParam(value = "The elements to get climate normals for as a comma-separated list of <a href=concepts#searchfilter>search filters</a>.")
    elements: Option[String],
    @ApiParam(value = "The validity period, e.g. '1931/1960'. If specified, only climate normals for this period will be returned.")
    period: Option[String],
    @ApiParam(value = "The output format of the result.",
      allowableValues = "jsonld",
      defaultValue = "jsonld")
    format: String) = no.met.security.AuthorizedAction { implicit request =>
    // scalastyle:on line.size.limit

    val start = DateTime.now(DateTimeZone.UTC) // start the clock

    Try  {
      // ensure that the query string contains supported fields only
      QueryStringUtil.ensureSubset(Set("sources", "elements", "period"), request.queryString.keySet)

      climateNormalsAccess.normals(ClimateNormalsQueryParameters(sources, elements, period))
    } match {
      case Success(data) =>
        if (data isEmpty) {
          Error.error(NOT_FOUND, Option("Could not find climate normals for this combination of query parameters"), None, start)
        } else {
          format.toLowerCase() match {
            case "jsonld" => Ok(new ClimateNormalsJsonFormat().format(start, data)) as "application/vnd.no.met.data.climatenormals-v0+json"
            case x        => Error.error(BAD_REQUEST, Some(s"Invalid output format: $x"), Some("Supported output formats: jsonld"), start)
          }
        }
      case Failure(x: BadRequestException) =>
        Error.error(BAD_REQUEST, Some(x getLocalizedMessage), x help, start)
      case Failure(x) => {
        //$COVERAGE-OFF$
        Logger.error(x.getLocalizedMessage)
        Error.error(INTERNAL_SERVER_ERROR, Some("An internal error occurred"), None, start)
        //$COVERAGE-ON$
      }
    }
  }


  @ApiOperation(
    value = "Get available combinations of sources, elements, and periods for climate normals.",
    notes = "Get available combinations of sources, elements, and periods for climate normals. To be expanded.",
    response = classOf[models.ClimateNormalsAvailableResponse],
    httpMethod = "GET")
  @ApiResponses(Array(
    // scalastyle:off magic.number
    new ApiResponse(code = 400, message = "Invalid parameter value or malformed request."),
    new ApiResponse(code = 401, message = "Unauthorized client ID."),
    new ApiResponse(code = 404, message = "No data was found for this combination of query parameters."),
    new ApiResponse(code = 500, message = "Internal server error."))
    // scalastyle:on magic.number
  )
  def getAvailable( // scalastyle:ignore public.methods.have.type
    // scalastyle:off line.size.limit
    @ApiParam(value = "If specified, only combinations matching these sources may be included in the output. Enter a comma-separated list of sources of the form SN&lt;number&gt;. If omitted, any source will match.")
    sources: Option[String],
    @ApiParam(value = "If specified, only combinations matching these elements may be included in the output. Enter a comma-separated list of element names in the form of <a href=concepts#searchfilter>search filters</a>.  If omitted, any element will match.")
    elements: Option[String],
    @ApiParam(value = "If specified, only combinations matching these validity periods may be included in the output. Enter a comma-separated list of validity period of the form '&lt;from year&gt;/&lt;to year&gt;', e.g. '1931/1960'.  If omitted, any period will match.")
    periods: Option[String],
    @ApiParam(value = "Specifies what information to return as a comma-separated list of 'sourceid', 'elementid', or 'period'. For example, 'sourceid,period' specifies that only source IDs and periods will appear in the output. If omitted, all fields are returned.")
    fields: Option[String],
    @ApiParam(value = "The output format of the result.",
      allowableValues = "jsonld",
      defaultValue = "jsonld")
    format: String) = no.met.security.AuthorizedAction { implicit request =>
    // scalastyle:on line.size.limit

    val start = DateTime.now(DateTimeZone.UTC) // start the clock

    Try  {
      // ensure that the query string contains supported fields only
      QueryStringUtil.ensureSubset(Set("sources", "elements", "periods", "fields"), request.queryString.keySet)

      climateNormalsAccess.available(ClimateNormalsAvailableQueryParameters(sources, elements, periods, fields))
    } match {
      case Success(data) =>
        if (data isEmpty) {
          Error.error(NOT_FOUND, Option("Could not find climate normals for this combination of query parameters"), None, start)
        } else {
          format.toLowerCase() match {
            case "jsonld" => Ok(
              new ClimateNormalsAvailableJsonFormat().format(start, data)) as "application/vnd.no.met.data.climatenormals.available-v0+json"
            case x => Error.error(BAD_REQUEST, Some(s"Invalid output format: $x"), Some("Supported output formats: jsonld"), start)
          }
        }
      case Failure(x: BadRequestException) =>
        Error.error(BAD_REQUEST, Some(x getLocalizedMessage), x help, start)
      case Failure(x) => {
        //$COVERAGE-OFF$
        Logger.error(x.getLocalizedMessage)
        Error.error(INTERNAL_SERVER_ERROR, Some("An internal error occurred"), None, start)
        //$COVERAGE-ON$
      }
    }
  }

}
