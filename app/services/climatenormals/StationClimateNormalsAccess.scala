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

  // supported element legacy codes
  private val monthLegacyCodes = List("TANM", "TAXM", "UM", "TAM_DAY_STDEV")
  private val dayLegacyCodes = List("TAM", "RR_ACC")

  private val normalsTableAlias = "nta"


  // Generates the element part of the WHERE clause.
  private def getElementQ(elements: Option[String], legacyCodes: List[String]): String = {
    // NOTE: For now we only support the provided legacyCodes directly in the elements parameter in the query string.
    // Later, the user should be allowed to specify alternative names, but then a translation from the elements database
    // (possibly indirectly via the elements/ service) is required.

    elements match {
      case None => { // ignore element filtering, i.e. select all available elements
        s"$normalsTableAlias.elem_code IN (${legacyCodes.map(s => s"'$s'").mkString(",") })"
      }
      case Some(x) => { // add an OR-expression for every legacy code that matches at least one of the input elements
        val elemList = x.split(",").map(_.trim)
        val result = legacyCodes.foldLeft("") { (acc, cur) =>
          acc + s"${
            if (elemList.exists(e => cur.toLowerCase.matches(e.toLowerCase.replace("*", ".*"))))
              s"${if (acc == "") "" else " OR "}$normalsTableAlias.elem_code='$cur'"
            else
              ""
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
          elementid,
          validfrom,
          validto,
          month,
          day,
          normal
        )
      }
    }

    private def getMainQuery(
      table: String, sourceQ: String, elemQ: String, validFromQ: String, validToQ: String, monthQ: String, dayQ: Option[String] = None) = {
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
         |    ${dayQ match { case Some(x) => s"$normalsTableAlias.day"; case None => "NULL" }} AS day,
         |    $normalsTableAlias.normal AS normal
         |  FROM $table $normalsTableAlias
         |  WHERE $sourceQ AND $elemQ AND $validFromQ AND $validToQ AND $monthQ${dayQ match { case Some(x) => s" AND $x"; case None => "" }}
         |) t1
         |ORDER BY sourceid, elementid, validfrom, validto, month${dayQ match {case Some(x) => ", day"; case None => "" }}
      """.stripMargin
    }

    // Returns a sorted list of integers from an input string of the form "1,3,5-7" (i.e. a comma-separated list of integers or integer ranges).
    private def getIntList(os: Option[String], min: Int, max: Int, itype: String): List[Int] = {

      def toIntSet(s: String): Set[Int] = {
        val psingle = "([0-9]+)".r
        val prange = "([0-9]+)-([0-9]+)".r
        s.trim match {
          case prange(a,b) => (a.toInt to b.toInt).toSet
          case psingle(a) => Set[Int](a.toInt)
          case _ => throw new BadRequestException(s"$itype: syntax error: $s (not of the form <int> or <int1>-<int2>)")
        }
      }

      os match {
        case Some(s) => {
          val list = s.split(",").foldLeft(Set[Int]()) { (acc, cur) => acc ++ toIntSet(cur) }.toList.sorted
          if (list.nonEmpty && ((list.head < min) || (list.last > max))) {
            throw new BadRequestException(s"$itype outside valid range [$min, $max]")
          }
          list
        }
        case None => List[Int]()
      }
    }

    def apply(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = {

      val stations = SourceSpecification(Some(qp.sources), Some(StationConfig.typeName)).stationNumbers
      assert(stations.nonEmpty)
      ensureStationsAreNonNegativeInts(stations)

      val sourceQ = s"stnr IN (${stations.mkString(",")})"

      val elemMonthQ = getElementQ(qp.elements, monthLegacyCodes)
      val elemDayQ = getElementQ(qp.elements, dayLegacyCodes)

      if ((elemMonthQ.toUpperCase == "FALSE") && (elemDayQ.toUpperCase == "FALSE")) {
        throw new BadRequestException(
          s"No valid elements specified",
          Some(s"valid elements for month normals: ${monthLegacyCodes.mkString(", ")}; valid elements for day normals: ${dayLegacyCodes.mkString(", ")}"))
      }

      val validFromQ = getValidYearBoundQ(qp.validFrom, "validfrom")
      val validToQ = getValidYearBoundQ(qp.validTo, "validto")

      val months = getIntList(qp.months, 1, 12, "months")
      val monthQ = if (months.isEmpty) "TRUE" else s"month IN (${months.mkString(",")})"

      val days = getIntList(qp.days, 1, 31, "days")
      val dayQ = if (days.isEmpty) "TRUE" else s"day IN (${days.mkString(",")})"

      val monthNormals = DB.withConnection("kdvh") { implicit connection =>
        val queryMonth = getMainQuery("t_normal_month", sourceQ, elemMonthQ, validFromQ, validToQ, monthQ)
        //Logger.debug("--------- queryMonth:")
        //Logger.debug(queryMonth)
        SQL(queryMonth).as( parser * )
      }

      val dayNormals = DB.withConnection("kdvh") { implicit connection =>
        val queryDay = getMainQuery("t_normal_diurnal", sourceQ, elemDayQ, validFromQ, validToQ, monthQ, Some(dayQ))
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
          elementid,
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

      val elemMonthQ = getElementQ(qp.elements, monthLegacyCodes)
      val elemDayQ = getElementQ(qp.elements, dayLegacyCodes)

      if ((elemMonthQ.toUpperCase == "FALSE") && (elemDayQ.toUpperCase == "FALSE")) {
        throw new BadRequestException(
          s"No valid elements specified",
          Some(s"valid elements for month normals: ${monthLegacyCodes.mkString(", ")}; valid elements for day normals: ${dayLegacyCodes.mkString(", ")}"))
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
  override def monthElements(): List[ClimateNormalsMonthElement] = monthLegacyCodes.map(ClimateNormalsMonthElement(_))
  override def dayElements(): List[ClimateNormalsDayElement] = dayLegacyCodes.map(ClimateNormalsDayElement(_))
}

//$COVERAGE-ON$
