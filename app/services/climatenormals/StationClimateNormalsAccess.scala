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
import models.ClimateNormal
import no.met.data._

import play.Logger


//$COVERAGE-OFF$ Not testing database queries

/**
 * Overall IDF access from stations.
 */
class StationClimateNormalsAccess extends ProdClimateNormalsAccess {

  // Generates the element part of the WHERE clause.
  private def getElementQ(elements: Option[String], tableAlias: String, legacyCodes: List[String]): String = {
    // NOTE: For now we only support the provided legacyCodes directly in the elements parameter in the query string.
    // Later, the user should be allowed to specify alternative names, but then a translation from the elements database
    // (possibly indirectly via the elements/ service) is required.

    elements match {
      case None => { // ignore element filtering, i.e. select all available elements
        s"$tableAlias.elem_code IN (${legacyCodes.map(s => s"'$s'").mkString(",") })"
      }
      case Some(x) => { // add an OR-expression for every legacy code that matches at least one of the input elements
        val elemList = x.split(",")
        val result = legacyCodes.foldLeft("") { (acc, cur) =>
          acc + s"${
            if (elemList.exists(e => cur.toLowerCase.matches(e.toLowerCase.replace("*", ".*"))))
              s"${if (acc == "") "" else " OR "}$tableAlias.elem_code='$cur'"
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

  private def getMainQuery(
    table: String, tableAlias: String, sourceQ: String, elemQ: String, validFromQ: String, validToQ: String, monthQ: String, dayQ: Option[String] = None) = {
    s"""
       |SELECT
       |  *
       |FROM (
       |  SELECT
       |    'SN' || $tableAlias.stnr AS sourceid,
       |    $tableAlias.elem_code AS elementid,
       |    $tableAlias.fyear AS validfrom,
       |    $tableAlias.tyear AS validto,
       |    $tableAlias.month AS month,
       |    ${dayQ match { case Some(x) => s"$tableAlias.day"; case None => "NULL" }} AS day,
       |    $tableAlias.normal AS normal
       |  FROM $table $tableAlias
       |  WHERE $sourceQ AND $elemQ AND $validFromQ AND $validToQ AND $monthQ${dayQ match { case Some(x) => s" AND $x"; case None => "" }}
       |) t1
       |ORDER BY sourceid, elementid, validfrom, validto, month${dayQ match {case Some(x) => ", day"; case None => "" }}
      """.stripMargin
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

    // scalastyle:off method.length
    // scalastyle:off cyclomatic.complexity
    def apply(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = {

      val stations = SourceSpecification(Some(qp.sources), Some(StationConfig.typeName)).stationNumbers
      assert(stations.nonEmpty)

      // for now, check at this point that stations contains only non-negative integeres (and not for example '18700:1' or '18700:all')
      // (later, SourceSpecification such a restriction could be included in the SourceSpecification API ... TBD)
      stations.foreach(s => {
        Try{val x = s.toInt; if (x < 0) throw new Exception } match {
          case Failure(e) => throw new BadRequestException(
            s"Invalid station number: $s (note that sensor channels are currently not supported for climate normals)")
          case Success(_) =>
        }
      })

      val sourceQ = s"stnr IN (${stations.mkString(",")})"

      val normalTableAlias = "nta"

      val monthLegacyCodes = List("TANM", "TAXM", "UM", "TAM_DAY_STDEV")
      val elemMonthQ = getElementQ(qp.elements, normalTableAlias, monthLegacyCodes)

      val dayLegacyCodes = List("TAM", "RR_ACC")
      val elemDayQ = getElementQ(qp.elements, normalTableAlias, dayLegacyCodes)

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


      // --- BEGIN month normals ----------------
      val monthNormals = DB.withConnection("kdvh") { implicit connection =>
        val queryMonth = getMainQuery("t_normal_month", normalTableAlias, sourceQ, elemMonthQ, validFromQ, validToQ, monthQ)
        //Logger.debug("--------- queryMonth:")
        //Logger.debug(queryMonth)
        SQL(queryMonth).as( parser * )
      }
//      val monthNormals = List[ClimateNormal](
//        ClimateNormal(Some("dummySourceID_m"), Some("dummyElementID_m"), Some(101), Some(102), Some(13), Some(32), 12.34)
//      )
      // --- END month normals ----------------


      // --- BEGIN day normals ----------------
      val dayNormals = DB.withConnection("kdvh") { implicit connection =>
        val queryDay = getMainQuery("t_normal_diurnal", normalTableAlias, sourceQ, elemDayQ, validFromQ, validToQ, monthQ, Some(dayQ))
        //Logger.debug("--------- queryDay:")
        //Logger.debug(queryDay)
        SQL(queryDay).as( parser * )
      }
//      val dayNormals = List[ClimateNormal](
//        ClimateNormal(Some("dummySourceID_d"), Some("dummyElementID_d"), Some(201), Some(202), Some(14), Some(33), 56.78)
//      )
      // --- END day normals ----------------


      monthNormals ++ dayNormals

    }
    // scalastyle:on method.length
    // scalastyle:on cyclomatic.complexity
  }

  override def normals(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = climateNormalsExec(qp)
}

//$COVERAGE-ON$
