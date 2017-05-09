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


/**
  * Mocked climate normals access.
  */
class MockClimateNormalsAccess extends ClimateNormalsAccess {

  // scalastyle:off magic.number

  def normals(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = {
    val srcSpec = SourceSpecification(Some(qp.sources), Some(StationConfig.typeName))
    val stationNumbers = srcSpec.stationNumbers
    List(new ClimateNormal("SN18700", "mean(min(air_temperature P1D) P1M)", 1931, 1960, 3, Some(31), 5.2))
  }

  def sources(qp: ClimateNormalsSourcesQueryParameters): List[ClimateNormalsSource] = {
    val srcSpec = SourceSpecification(qp.sources, Some(StationConfig.typeName))
    val stationNumbers = srcSpec.stationNumbers
    List(new ClimateNormalsSource("SN18700", "mean(min(air_temperature P1D) P1M)", 1931, 1960))
  }

  def elements(): List[ClimateNormalsElement] = {
    List(new ClimateNormalsElement("mean(min(air_temperature P1D) P1M)"))
  }

  // scalastyle:on magic.number
}
