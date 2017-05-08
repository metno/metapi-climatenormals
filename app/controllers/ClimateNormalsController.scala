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
    @ApiParam(value = "The elements to get climate normals for as a comma-separated list of <a href=concepts#searchfilter>search filters</a>. For now, only legacy codes are supported. Use separate resources to get available legacy codes for <a href=reference#/climatenormals/getMonthElements>month</a> and <a href=reference#/climatenormals/getDayElements>day</a> normals. Use the <a href=reference#/elements>elements</a> resource to translate legacy codes to alternative names.")
    elements: Option[String],
    @ApiParam(value = "The start year of the validity period as a four-digit integer, e.g. '1955'. If specified, climate normals valid only before this year will not be returned.")
    validfrom: Option[String],
    @ApiParam(value = "The end year of the validity period as a four-digit integer, e.g. '1975'. If specified, climate normals valid only after this year will not be returned.")
    validto: Option[String],
    @ApiParam(value = "The months to get climate normals for as a comma-separated list of integers or integer ranges between 1 and 12, e.g. '1,5,8-12'. If left out, climate normals for all available months are returned.")
    months: Option[String],
    @ApiParam(value = "The days to get climate normals for as a comma-separated list of integers or integer ranges between 1 and 31, e.g. '2-7,5,25-31'. If left out, climate normals for all available days are returned.")
    days: Option[String],
    @ApiParam(value = "The output format of the result.",
      allowableValues = "jsonld",
      defaultValue = "jsonld")
    format: String) = no.met.security.AuthorizedAction { implicit request =>
    // scalastyle:on line.size.limit

    val start = DateTime.now(DateTimeZone.UTC) // start the clock

    Try  {
      // ensure that the query string contains supported fields only
      QueryStringUtil.ensureSubset(Set("sources", "elements", "validfrom", "validto", "months", "days"), request.queryString.keySet)

      climateNormalsAccess.normals(ClimateNormalsQueryParameters(sources, elements, validfrom, validto, months, days))
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
    value = "Get available sources for climate normals.",
    notes = "Get available sources for climate normals. To be expanded.",
    response = classOf[models.ClimateNormalsSourcesResponse],
    httpMethod = "GET")
  @ApiResponses(Array(
    // scalastyle:off magic.number
    new ApiResponse(code = 400, message = "Invalid parameter value or malformed request."),
    new ApiResponse(code = 401, message = "Unauthorized client ID."),
    new ApiResponse(code = 404, message = "No data was found for this combination of query parameters."),
    new ApiResponse(code = 500, message = "Internal server error."))
    // scalastyle:on magic.number
  )
  def getSources( // scalastyle:ignore public.methods.have.type
    // scalastyle:off line.size.limit
    @ApiParam(value = "The sources to get information for as a comma-separated list. Each source should be of the form SN&lt;number&gt;. If left out, all available sources are considered for output.")
    sources: Option[String],
    @ApiParam(value = "The elements that the sources must provide normals for as a comma-separated list of <a href=concepts#searchfilter>search filters</a>. For now, only legacy codes are supported. Use separate resources to get available legacy codes for <a href=reference#/climatenormals/getMonthElements>month</a> and <a href=reference#/climatenormals/getDayElements>day</a> normals. Use the <a href=reference#/elements>elements</a> resource to translate legacy codes to alternative names.")
    elements: Option[String],
    @ApiParam(value = "The start year of the validity period as a four-digit integer, e.g. '1955'. If specified, a source will not be considered for output if all its climate normals are valid only before this year.")
    validfrom: Option[String],
    @ApiParam(value = "The end year of the validity period as a four-digit integer, e.g. '1975'. If specified, a source will not be considered for output if all its climate normals are valid only after this year.")
    validto: Option[String],
    @ApiParam(value = "The output format of the result.",
      allowableValues = "jsonld",
      defaultValue = "jsonld")
    format: String) = no.met.security.AuthorizedAction { implicit request =>
    // scalastyle:on line.size.limit

    val start = DateTime.now(DateTimeZone.UTC) // start the clock

    Try  {
      // ensure that the query string contains supported fields only
      QueryStringUtil.ensureSubset(Set("sources", "elements", "validfrom", "validto"), request.queryString.keySet)

      climateNormalsAccess.sources(ClimateNormalsSourcesQueryParameters(sources, elements, validfrom, validto))
    } match {
      case Success(data) =>
        if (data isEmpty) {
          Error.error(NOT_FOUND, Option("Could not find climate normals for this combination of query parameters"), None, start)
        } else {
          format.toLowerCase() match {
            case "jsonld" => Ok(
              new ClimateNormalsSourcesJsonFormat().format(start, data)) as "application/vnd.no.met.data.climatenormals.availablesources-v0+json"
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


  @ApiOperation(
    value = "Get available month elements for climate normals.",
    notes = "Get available month elements for climate normals. To be expanded.",
    response = classOf[models.ClimateNormalsMonthElementsResponse],
    httpMethod = "GET")
  @ApiResponses(Array(
    // scalastyle:off magic.number
    new ApiResponse(code = 400, message = "Invalid parameter value or malformed request."),
    new ApiResponse(code = 401, message = "Unauthorized client ID."),
    new ApiResponse(code = 404, message = "No data was found for this combination of query parameters."),
    new ApiResponse(code = 500, message = "Internal server error."))
    // scalastyle:on magic.number
  )
  def getMonthElements( // scalastyle:ignore public.methods.have.type
    // scalastyle:off line.size.limit
    @ApiParam(value = "The output format of the result.",
      allowableValues = "jsonld",
      defaultValue = "jsonld")
    format: String) = no.met.security.AuthorizedAction { implicit request =>
    // scalastyle:on line.size.limit

    val start = DateTime.now(DateTimeZone.UTC) // start the clock

    Try  {
      // ensure that the query string contains supported fields only
      QueryStringUtil.ensureSubset(Set(), request.queryString.keySet)

      climateNormalsAccess.monthElements()
    } match {
      case Success(data) =>
        if (data isEmpty) {
          Error.error(NOT_FOUND, Option("No month elements available"), None, start)
        } else {
          format.toLowerCase() match {
            case "jsonld" => Ok(
              new ClimateNormalsMonthElementsJsonFormat().format(start, data)) as "application/vnd.no.met.data.climatenormals.availablemonthelements-v0+json"
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

  @ApiOperation(
    value = "Get available day elements for climate normals.",
    notes = "Get available day elements for climate normals. To be expanded.",
    response = classOf[models.ClimateNormalsDayElementsResponse],
    httpMethod = "GET")
  @ApiResponses(Array(
    // scalastyle:off magic.number
    new ApiResponse(code = 400, message = "Invalid parameter value or malformed request."),
    new ApiResponse(code = 401, message = "Unauthorized client ID."),
    new ApiResponse(code = 404, message = "No data was found for this combination of query parameters."),
    new ApiResponse(code = 500, message = "Internal server error."))
    // scalastyle:on magic.number
  )
  def getDayElements( // scalastyle:ignore public.methods.have.type
    // scalastyle:off line.size.limit
    @ApiParam(value = "The output format of the result.",
      allowableValues = "jsonld",
      defaultValue = "jsonld")
    format: String) = no.met.security.AuthorizedAction { implicit request =>
    // scalastyle:on line.size.limit

    val start = DateTime.now(DateTimeZone.UTC) // start the clock

    Try  {
      // ensure that the query string contains supported fields only
      QueryStringUtil.ensureSubset(Set(), request.queryString.keySet)

      climateNormalsAccess.dayElements()
    } match {
      case Success(data) =>
        if (data isEmpty) {
          Error.error(NOT_FOUND, Option("No day elements available"), None, start)
        } else {
          format.toLowerCase() match {
            case "jsonld" => Ok(
              new ClimateNormalsDayElementsJsonFormat().format(start, data)) as "application/vnd.no.met.data.climatenormals.availabledayelements-v0+json"
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
