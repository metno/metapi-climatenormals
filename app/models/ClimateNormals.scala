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

package models

import io.swagger.annotations._
import scala.annotation.meta.field
import java.net.URL
import com.github.nscala_time.time.Imports._
import no.met.data.{ApiConstants,BasicResponse}
import no.met.geometry.Point

@ApiModel(description="Data response for climate normals")
case class ClimateNormalsResponse(
  @(ApiModelProperty @field)(name=ApiConstants.CONTEXT_NAME, value=ApiConstants.CONTEXT, example=ApiConstants.METAPI_CONTEXT) context: URL,
  @(ApiModelProperty @field)(name=ApiConstants.OBJECT_TYPE_NAME, value=ApiConstants.OBJECT_TYPE, example="ClimateNormalsResponse") responseType: String,
  @(ApiModelProperty @field)(value=ApiConstants.API_VERSION, example=ApiConstants.API_VERSION_EXAMPLE) apiVersion: String,
  @(ApiModelProperty @field)(value=ApiConstants.LICENSE, example=ApiConstants.METAPI_LICENSE) license: URL,
  @(ApiModelProperty @field)(value=ApiConstants.CREATED_AT, dataType="String", example=ApiConstants.CREATED_AT_EXAMPLE) createdAt: DateTime,
  @(ApiModelProperty @field)(value=ApiConstants.QUERY_TIME, dataType="String", example=ApiConstants.QUERY_TIME_EXAMPLE) queryTime: Duration,
  @(ApiModelProperty @field)(value=ApiConstants.CURRENT_ITEM_COUNT, example=ApiConstants.CURRENT_ITEM_COUNT_EXAMPLE) currentItemCount: Long,
  @(ApiModelProperty @field)(value=ApiConstants.ITEMS_PER_PAGE, example=ApiConstants.ITEMS_PER_PAGE_EXAMPLE) itemsPerPage: Long,
  @(ApiModelProperty @field)(value=ApiConstants.OFFSET, example=ApiConstants.OFFSET_EXAMPLE) offset: Long,
  @(ApiModelProperty @field)(value=ApiConstants.TOTAL_ITEM_COUNT, example=ApiConstants.TOTAL_ITEM_COUNT_EXAMPLE) totalItemCount: Long,
  @(ApiModelProperty @field)(value=ApiConstants.NEXT_LINK, example=ApiConstants.NEXT_LINK_EXAMPLE) nextLink: Option[URL],
  @(ApiModelProperty @field)(value=ApiConstants.PREVIOUS_LINK, example=ApiConstants.PREVIOUS_LINK_EXAMPLE) previousLink: Option[URL],
  @(ApiModelProperty @field)(value=ApiConstants.CURRENT_LINK, example=ApiConstants.CURRENT_LINK_EXAMPLE) currentLink: URL,
  @(ApiModelProperty @field)(value=ApiConstants.DATA) data: Seq[ClimateNormal]
)
extends BasicResponse(
  context, responseType, apiVersion, license, createdAt, queryTime, currentItemCount, itemsPerPage, offset, totalItemCount, nextLink, previousLink, currentLink)

@ApiModel(description="A single climate normal value with metadata")
case class ClimateNormal(
    @(ApiModelProperty @field)(value="Source id.", example="SN18700") sourceId: String,
    @(ApiModelProperty @field)(value="Element id.", example="mean(min(air_temperature P1D) P1M)") elementId: String,
    @(ApiModelProperty @field)(value="Validity period.", example="1961/1990") period: String,
    @(ApiModelProperty @field)(value="Month.", example="3") month: Int,
    @(ApiModelProperty @field)(value="Day of month (omitted if n/a).", example="31") day: Option[Int],
    @(ApiModelProperty @field)(value="Normal value (omitted if not found).", example="5.2") normal: Option[Double]
)
