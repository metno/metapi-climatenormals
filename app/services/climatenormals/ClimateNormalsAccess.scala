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

package services.climatenormals

import models.ClimateNormal
import scala.util._
import no.met.data._
import no.met.geometry._


/**
 * Holds query string parameters from the original request.
 */
case class ClimateNormalsQueryParameters(
  sources: String, elements: Option[String] = None, validFrom: Option[String] = None, validTo: Option[String] = None,
  months: Option[String] = None, days: Option[String] = None)


/**
  * Interface for overall climate normals access. Implementations of this interface are injected in the controller in either production or
  * development mode.
  */
trait ClimateNormalsAccess {

  /**
    * Extracts climate normals based on query parameters.
    */
  def normals(qp: ClimateNormalsQueryParameters): List[ClimateNormal]
}
