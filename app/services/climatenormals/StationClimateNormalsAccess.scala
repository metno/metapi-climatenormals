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
    "standard_deviation(mean(air_temperature P1D) P1M)" -> "TAM_DAY_STDEV",
    "mean(air_temperature P1M)" -> "TAM",
    "sum(precipitation_amount P1M)" -> "RR",
    "mean(air_pressure_at_sea_level P1M)" -> "PRM",
    "mean(surface_air_pressure P1M)" -> "POM",
    "sum(duration_of_sunshine P1M)" -> "OT",
    "integral_of_excess_interpolated(mean(air_temperature P1D) P1M 17.0)" -> "GD17_I",
    "number_of_days_gte(sum(precipitation_amount P1D) P1M 0.1)" -> "DRR_GE1"
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
            if (elemList.exists(e => cur.toLowerCase.matches(e.toLowerCase.replace("*", ".*").replace("(", "\\(").replace(")", "\\)")))) {
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
        get[Option[Double]]("normal") map {
        case sourceid~elementid~validfrom~validto~month~day~normal
        => ClimateNormal(
          sourceid,
          fromLegacyElem(elementid),
          s"$validfrom/$validto",
          month,
          day,
          normal
        )
      }
    }

    // Generates the period matching part of the WHERE clause.
    private def getPeriodQ(s: Option[String]): String = {
      s match {
        case None => "TRUE"
        //case Some(ps) => s"'${ps}'=${normalsTableAlias}.fyear::text||'/'||${normalsTableAlias}.tyear::text"
        case Some(ps) => {
          val validPeriod = s"""(\\d\\d\\d\\d/\\d\\d\\d\\d)""".r
          s"${normalsTableAlias}.fyear::text||'/'||${normalsTableAlias}.tyear::text = '${
            ps.trim match {
              case validPeriod(vp) => vp
              case _ => throw new BadRequestException(s"Invalid period", Some(s"Supported format: yyyy/yyyy"))
            }}'"
        }
      }
    }

    private def getMainQuery(
      table: String, sourceQ: String, elemQ: String, periodQ: String, hasDay: Boolean) = {
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
         |  WHERE $sourceQ AND $elemQ AND $periodQ
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

      val periodQ = getPeriodQ(qp.period)

      val monthNormals = {
        val queryMonth = getMainQuery("t_normal_month", sourceQ, elemMonthQ, periodQ, false)
//        Logger.debug("--------- queryMonth:")
//        Logger.debug(queryMonth)
        Cache().get[ClimateNormal]("kdvh", queryMonth, parser)
      }

      val dayNormals = {
        val queryDay = getMainQuery("t_normal_diurnal", sourceQ, elemDayQ, periodQ, true)
//        Logger.debug("--------- queryDay:")
//        Logger.debug(queryDay)
        Cache().get[ClimateNormal]("kdvh", queryDay, parser)
      }

      monthNormals ++ dayNormals
    }
  }


  private object climateNormalsAvailableExec {

    private val parser: RowParser[ClimateNormalsAvailable] = {
      get[String]("sourceid") ~
        get[String]("elementid") ~
        get[Int]("validfrom") ~
        get[Int]("validto") map {
        case sourceid~elementid~validfrom~validto
        => ClimateNormalsAvailable(
          Some(sourceid),
          Some(fromLegacyElem(elementid)),
          Some(s"$validfrom/$validto")
        )
      }
    }

    // Generates the periods matching part of the WHERE clause.
    private def getPeriodsQ(s: Option[String]): String = {
      s match {
        case None => "TRUE"
        case Some(ps) => {
          val validPeriod = s"""(\\d\\d\\d\\d/\\d\\d\\d\\d)""".r
          s"${normalsTableAlias}.fyear::text||'/'||${normalsTableAlias}.tyear::text IN (${
            ps.split(",").map(p => s"'${p.trim match {
              case validPeriod(vp) => vp
              case _ => throw new BadRequestException(s"Invalid period", Some(s"Supported format: yyyy/yyyy"))
            }}'").mkString(", ")
          })"
        }
      }
    }

    private def getMainQuery(table: String, sourceQ: String, elemQ: String, periodsQ: String) = {
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
         |  WHERE $sourceQ AND $elemQ AND $periodsQ
         |) t1
         |ORDER BY sourceid, elementid, validfrom, validto
      """.stripMargin
    }

    def apply(qp: ClimateNormalsAvailableQueryParameters): List[ClimateNormalsAvailable] = {

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

      val periodsQ = getPeriodsQ(qp.periods)

      val fields :Set[String] = FieldSpecification.parse(qp.fields)
      val suppFields = Set("sourceid", "elementid", "period")
      fields.foreach(f => if (!suppFields.contains(f)) throw new BadRequestException(s"Unsupported field: $f", Some(s"Supported fields: ${suppFields.mkString(", ")}")))

      val availableMonthCombos = {
        val queryMonth = getMainQuery("t_normal_month", sourceQ, elemMonthQ, periodsQ)
//        Logger.debug("--------- queryMonth:")
//        Logger.debug(queryMonth)
        Cache().get[ClimateNormalsAvailable]("kdvh", queryMonth, parser)
      }

      val availableDayCombos = {
        val queryDay = getMainQuery("t_normal_diurnal", sourceQ, elemDayQ, periodsQ)
//        Logger.debug("--------- queryDay:")
//        Logger.debug(queryDay)
        Cache().get[ClimateNormalsAvailable]("kdvh", queryDay, parser)
      }

      // filter combos on fields

      val omitSourceId = fields.nonEmpty && !fields.contains("sourceid")
      val omitElementId = fields.nonEmpty && !fields.contains("elementid")
      val omitPeriod = fields.nonEmpty && !fields.contains("period")

      val combos = availableMonthCombos ++ availableDayCombos
      combos.map(s => s.copy(
        sourceId = if (omitSourceId) None else s.sourceId,
        elementId = if (omitElementId) None else s.elementId,
        period = if (omitPeriod) None else s.period
      )).distinct.sortBy(s => (s.sourceId, s.elementId, s.period))
    }
  }

  private object Cache {
    // scalastyle:off magic.number

    // Set the maximum number of items that the cache will accommodate. Upon cache miss, the least recently used item will be thrown out first.
    // Note the tradeoff between cache performance and memory usage (more items means fewer cache misses).
    val maxItems: Int = current.configuration.getInt("dbcache.climatenormals.maxitems").getOrElse(100)

    // Set the maximum number of seconds an item is allowed to stay in the cache without being refreshed from the database.
    // Note the tradeoff between cache performance and getting up-to-date information (database updates relevant to an item may take up to
    // expireSecs seconds before being reflected in the cache).
    val expireSecs: Int = current.configuration.getInt("dbcache.climatenormals.expiresecs").getOrElse({ val secsIn24H = 86400; secsIn24H })

    // scalastyle:on magic.number
    val instance = new DBCache(maxItems, expireSecs)
    def apply(): DBCache = instance
  }


  override def normals(qp: ClimateNormalsQueryParameters): List[ClimateNormal] = climateNormalsExec(qp)
  override def available(qp: ClimateNormalsAvailableQueryParameters): List[ClimateNormalsAvailable] = climateNormalsAvailableExec(qp)
}

//$COVERAGE-ON$
