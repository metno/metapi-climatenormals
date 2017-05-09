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

import scala.language.postfixOps
import play.api.Play.current
import play.api.db._
import anorm._
import anorm.SqlParser._
import util.{ Try, Success, Failure }
import models._
import no.met.data._

import play.Logger


//$COVERAGE-OFF$ Not testing database queries

/**
 * Overall climate normals access from station sources.
 */
class StationClimateNormalsAccess extends ProdClimateNormalsAccess {

  // supported element IDs with corresponding legacy codes (### hard-coded for now)
  private val monthElemMap: Map[String, String] = Map(
    "mean(min(air_temperature P1D) P1M)" -> "TANM",
    "mean(max(air_temperature P1D) P1M)" -> "TAXM",
    "mean(relative_humidity P1M)" -> "UM",
    "standard_deviation(mean(air_temperature P1D) P1M)" -> "TAM_DAY_STDEV"
  )
  private val invMonthElemMap = monthElemMap.map(_.swap) // ### WARNING: This assumes that monthElemMap is a one-to-one relation
  //
  private val dayElemMap: Map[String, String] = Map(
    "mean(air_temperature P1D)" -> "TAM",
    "sum_until_day_of_year(precipitation_amount P1D)" -> "RR_ACC"
  )
  private val invDayElemMap = dayElemMap.map(_.swap) // ### WARNING: This assumes that dayElemMap is a one-to-one relation
  //
  private def fromLegacyElem(legacyElem: String) = if (invMonthElemMap.contains(legacyElem)) invMonthElemMap(legacyElem) else invDayElemMap(legacyElem)

  private val normalsTableAlias = "nta"


  // Generates the element part of the WHERE clause.
  private def getElementQ(elements: Option[String], suppElems: Map[String, String]): String = {
    elements match {
      case None => { // ignore element filtering, i.e. select all available elements
        s"$normalsTableAlias.elem_code IN (${suppElems.values.toSet.toList.map((s: String) => s"'$s'").mkString(",") })"
      }
      case Some(x) => { // add an OR-expression for every legacy code that matches at least one of the input elements
        val elemList = x.split(",").map(_.trim)
        val result = suppElems.keys.foldLeft("") { (acc, cur) =>
          acc + s"${
            if (elemList.exists(e => cur.toLowerCase.matches(e.toLowerCase.replace("*", ".*")))) {
              s"${if (acc == "") "" else " OR "}$normalsTableAlias.elem_code='${suppElems(cur)}'"
            } else {
              ""
            }
          }"
        }
        if (result == "") "FALSE" else s"($result)"
      }
    }
  }

  // Generates a year bound part of the WHERE clause. s is the year while btype is either "validfrom" or "validto".
  private def getValidYearBoundQ(s: Option[String], btype: String): String = {

    def parseYear(ys: String, min: Int, max: Int) = {
      val y = ys.toInt
      if (y < min || y > max) throw new Exception(s"year outside valid range [$min, $max]: $ys")
      y
    }

    s match {
      case None => "TRUE" // ignore this bound type
      case Some(ys) => {
        Try(parseYear(ys, 0, 9999)) match {
          case Failure(e) => throw new BadRequestException(s"Failed to parse $btype value: ${e.getMessage}")
          case Success(y) => s"$y ${if (btype.toLowerCase == "validfrom") "< tyear" else "> fyear"}"
        }
      }
    }
  }

  // For now, define a local function that checks that a list of stations contains only non-negative integeres
  // (and not for example '18700:1' or '18700:all'). Later, such a restriction could be included in the SourceSpecification API ... TBD
  private def ensureStationsAreNonNegativeInts(stations: Seq[String]) = {
    stations.foreach(s => {
      Try {
        val x = s.toInt; if (x < 0) throw new Exception
      } match {
        case Failure(e) => throw new BadRequestException(
          s"Invalid station number: $s (note that sensor channels are currently not supported for climate normals)")
        case Success(_) =>
      }
    })
  }


  private object climateNormalsExec {

    private val parser: RowParser[ClimateNormal] = {
      get[String]("sourceid") ~
        get[String]("elementid") ~
        get[Int]("validfrom") ~
        get[Int]("validto") ~
        get[Int]("month") ~
        get[Option[Int]]("day") ~
        get[Double]("normal") map {
        case sourceid~elementid~validfrom~validto~month~day~normal
        => ClimateNormal(
          sourceid,
          fromLegacyElem(elementid),
          validfrom,
          validto,
          month,
          day,
          normal
        )
      }
    }

    private def getMainQuery(
      table: String, sourceQ: String, elemQ: String, validFromQ: String, validToQ: String, hasDay: Boolean) = {
      s"""
         |SELECT
         |  *
         |FROM (
         |  SELECT
         |    'SN' || $normalsTableAlias.stnr AS sourceid,
         |    $normalsTableAlias.elem_code AS elementid,
         |    $normalsTableAlias.fyear AS validfrom,
         |    $normalsTableAlias.tyear AS validto,
         |    $normalsTableAlias.month AS month,
         |    ${if (hasDay) s"$normalsTableAlias.day" else "NULL"} AS day,
         |    $normalsTableAlias.normal AS normal
         |  FROM $table $normalsTableAlias
         |  WHERE $sourceQ AND $elemQ AND $validFromQ AND $validToQ
         |) t1
         |ORDER BY sourceid, elementid, validfrom, validto, month${if (hasDay) ", day" else "" }
      """.stripMargin
    }

    def apply(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = {

      val stations = SourceSpecification(Some(qp.sources), Some(StationConfig.typeName)).stationNumbers
      assert(stations.nonEmpty)
      ensureStationsAreNonNegativeInts(stations)

      val sourceQ = s"stnr IN (${stations.mkString(",")})"

      val elemMonthQ = getElementQ(qp.elements, monthElemMap)
      val elemDayQ = getElementQ(qp.elements, dayElemMap)

      if ((elemMonthQ.toUpperCase == "FALSE") && (elemDayQ.toUpperCase == "FALSE")) {
        throw new BadRequestException(
          s"No valid elements specified",
          Some(s"valid elements for month normals: ${monthElemMap.keys.mkString(", ")}; valid elements for day normals: ${dayElemMap.keys.mkString(", ")}"))
      }

      val validFromQ = getValidYearBoundQ(qp.validFrom, "validfrom")
      val validToQ = getValidYearBoundQ(qp.validTo, "validto")

      val monthNormals = DB.withConnection("kdvh") { implicit connection =>
        val queryMonth = getMainQuery("t_normal_month", sourceQ, elemMonthQ, validFromQ, validToQ, false)
        //Logger.debug("--------- queryMonth:")
        //Logger.debug(queryMonth)
        SQL(queryMonth).as( parser * )
      }

      val dayNormals = DB.withConnection("kdvh") { implicit connection =>
        val queryDay = getMainQuery("t_normal_diurnal", sourceQ, elemDayQ, validFromQ, validToQ, true)
        //Logger.debug("--------- queryDay:")
        //Logger.debug(queryDay)
        SQL(queryDay).as( parser * )
      }

      monthNormals ++ dayNormals
    }
  }


  private object climateNormalsSourcesExec {

    private val parser: RowParser[ClimateNormalsSource] = {
      get[String]("sourceid") ~
        get[String]("elementid") ~
        get[Int]("validfrom") ~
        get[Int]("validto") map {
        case sourceid~elementid~validfrom~validto
        => ClimateNormalsSource(
          sourceid,
          fromLegacyElem(elementid),
          validfrom,
          validto
        )
      }
    }

    private def getMainQuery(table: String, sourceQ: String, elemQ: String, validFromQ: String, validToQ: String) = {
      s"""
         |SELECT
         |  *
         |FROM (
         |  SELECT DISTINCT
         |    'SN' || $normalsTableAlias.stnr AS sourceid,
         |    $normalsTableAlias.elem_code AS elementid,
         |    $normalsTableAlias.fyear AS validfrom,
         |    $normalsTableAlias.tyear AS validto
         |  FROM $table $normalsTableAlias
         |  WHERE $sourceQ AND $elemQ AND $validFromQ AND $validToQ
         |) t1
         |ORDER BY sourceid, elementid, validfrom, validto
      """.stripMargin
    }

    def apply(qp: ClimateNormalsSourcesQueryParameters): List[ClimateNormalsSource] = {

      val stations = SourceSpecification(qp.sources, Some(StationConfig.typeName)).stationNumbers
      ensureStationsAreNonNegativeInts(stations)

      val sourceQ = if (stations.nonEmpty) s"stnr IN (${stations.mkString(",")})" else "TRUE"

      val elemMonthQ = getElementQ(qp.elements, monthElemMap)
      val elemDayQ = getElementQ(qp.elements, dayElemMap)

      if ((elemMonthQ.toUpperCase == "FALSE") && (elemDayQ.toUpperCase == "FALSE")) {
        throw new BadRequestException(
          s"No valid elements specified",
          Some(s"valid elements for month normals: ${monthElemMap.keys.mkString(", ")}; valid elements for day normals: ${dayElemMap.keys.mkString(", ")}"))
      }

      val validFromQ = getValidYearBoundQ(qp.validFrom, "validfrom")
      val validToQ = getValidYearBoundQ(qp.validTo, "validto")

      val monthSources = DB.withConnection("kdvh") { implicit connection =>
        val queryMonth = getMainQuery("t_normal_month", sourceQ, elemMonthQ, validFromQ, validToQ)
//        Logger.debug("--------- queryMonth:")
//        Logger.debug(queryMonth)
        SQL(queryMonth).as( parser * )
      }

      val daySources = DB.withConnection("kdvh") { implicit connection =>
        val queryDay = getMainQuery("t_normal_diurnal", sourceQ, elemDayQ, validFromQ, validToQ)
//        Logger.debug("--------- queryDay:")
//        Logger.debug(queryDay)
        SQL(queryDay).as( parser * )
      }

      monthSources ++ daySources
    }
  }


  override def normals(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = climateNormalsExec(qp)
  override def sources(qp: ClimateNormalsSourcesQueryParameters): List[ClimateNormalsSource] = climateNormalsSourcesExec(qp)
  override def elements(): List[ClimateNormalsElement] =
    monthElemMap.keys.toList.map(ClimateNormalsElement(_)) ++ dayElemMap.keys.toList.map(ClimateNormalsElement(_))
}

//$COVERAGE-ON$
