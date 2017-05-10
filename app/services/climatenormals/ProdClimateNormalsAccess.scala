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

import models._
import no.met.data._
import scala.util._


//$COVERAGE-OFF$ Not testing production mode

/**
  * Class to handle climate normals access in production mode. An instance of this class is injected in the controller. A request for an endpoint is
  * delegated to a subclass selected according to query parameters. Since only station sources are currently supported, the request is handled by an instance of
  * StationClimateNormalsAccess.
  */
class ProdClimateNormalsAccess extends ClimateNormalsAccess {
  def normals(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = ProdClimateNormalsAccess.stationAccess.normals(qp)
  def available(qp: ClimateNormalsAvailableQueryParameters): List[ClimateNormalsAvailable] = ProdClimateNormalsAccess.stationAccess.available(qp)
}


/**
  * Companion object that holds an implementation of ProdClimateNormalsAccess for station access.
  */
object ProdClimateNormalsAccess {
  lazy val stationAccess = new StationClimateNormalsAccess
}

//$COVERAGE-OFF$
