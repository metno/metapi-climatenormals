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

import play.Logger


//$COVERAGE-OFF$ Not testing database queries

// Simple LRU caching of DB queries.
object DBCache {

  // Returns the current time since 1970 in milliseconds.
  private def nowMillis: Long = System.currentTimeMillis

  private val maxItems = 10 // maximum number of items in cache - hardcoded for now, should be defined in configuration file etc. ... TBD
  private var cache: collection.mutable.Map[String, List[Any]] = collection.mutable.Map[String, List[Any]]()
  private var accessed: collection.mutable.Map[String, Long] = collection.mutable.Map[String, Long]()

  // Retrieves the (possibly cached) result of executing a query.
  def get[T](database: String, query: String, p: RowParser[T]): List[T] = {

    assert(cache.size <= maxItems)

    val origSize = cache.size
    var hit = false

    val value: List[T] =
      if (!cache.contains(query)) { // cache miss, so compute and cache value

        // TBD: expiration must also be used as a condition for refreshing the value!

        val v = DB.withConnection(database) { implicit connection => SQL(query).as( p * ) } // compute value from database

        if (cache.size == maxItems) { // make room by removing item for the least recently used query
        val lruQuery = accessed.minBy(_._2)._1
          accessed.remove(lruQuery)
          cache.remove(lruQuery)
        }
        assert(cache.size < maxItems)

        cache(query) = v // cache computed value
        v.asInstanceOf[List[T]]
      } else { // cache hit
        hit = true
        cache(query).asInstanceOf[List[T]]
      }

    accessed(query) = nowMillis // update access time
    //Logger.debug(s"hit: ${if (hit) 1 else 0}; items in cache: ${cache.size}; rows in value: ${value.size}")
    value // returned cached value
  }

}

//$COVERAGE-ON$
