import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from mcp.server.fastmcp import FastMCP
from typing import Optional

# -------------------------------------------------------
# Initialize MCP Server
# -------------------------------------------------------
mcp = FastMCP("weather-bq-extended")

# BigQuery client (uses GOOGLE_APPLICATION_CREDENTIALS)
KEY_PATH = os.path.join(os.path.dirname(__file__), "gcp-sa.json")

with open(KEY_PATH, "r") as f:
    info = json.load(f)

creds = service_account.Credentials.from_service_account_info(info)
bq_client = bigquery.Client(credentials=creds, project=info.get("project_id"))

# Public NOAA GSOD dataset on BigQuery
DATASET_TABLE = "bigquery-public-data.noaa_gsod.gsod*"
STATIONS_TABLE = "bigquery-public-data.noaa_gsod.stations"


def f_to_c(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return (v - 32.0) * 5.0 / 9.0


def inches_to_mm(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return v * 25.4


def knots_to_kmh(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return v * 1.852


@mcp.tool()
async def resolve_city(city: str, country_code: Optional[str] = None) -> dict:
    """
    Resolve a city into a weather station using NOAA GSOD stations.

    Returns station id (stn + wban), name, country, and coordinates.
    """
    params = [bigquery.ScalarQueryParameter("city", "STRING", city)]
    country_filter = ""
    if country_code:
        country_filter = "AND country = @country"
        params.append(
            bigquery.ScalarQueryParameter("country", "STRING", country_code)
        )

    query = f"""
    SELECT
      stn,
      wban,
      name,
      country,
      lat,
      lon
    FROM `{STATIONS_TABLE}`
    WHERE UPPER(name) LIKE CONCAT('%', UPPER(@city), '%')
      {country_filter}
    ORDER BY name
    LIMIT 1
    """

    job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    rows = list(job)
    if not rows:
        return {"found": False, "reason": "No station for that city"}

    row = rows[0]
    return {
        "found": True,
        "stn": row["stn"],
        "wban": row["wban"],
        "name": row["name"],
        "country": row["country"],
        "lat": row["lat"],
        "lon": row["lon"],
    }


@mcp.tool()
async def range_weather_summary(
    stn: str,
    wban: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Aggregate weather stats for a station between start_date and end_date (YYYY-MM-DD).

    Returns:
    - temperature: min / max / mean (°C)
    - rainfall: total precipitation (mm)
    - wind: mean wind speed (km/h)
    """

    query = f"""
    SELECT
      MIN(temp) AS temp_min_f,
      MAX(temp) AS temp_max_f,
      AVG(temp) AS temp_mean_f,
      SUM(IF(prcp < 99.99, prcp, 0)) AS prcp_sum_in,
      AVG(IF(wdsp < 999.9, wdsp, NULL)) AS wind_mean_knots
    FROM `{DATASET_TABLE}`
    WHERE stn = @stn
      AND wban = @wban
      AND date BETWEEN @start_date AND @end_date
      AND temp != 9999.9
    """

    job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("stn", "STRING", stn),
                bigquery.ScalarQueryParameter("wban", "STRING", wban),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        ),
    )
    rows = list(job)
    if not rows:
        return {"found": False, "reason": "No data in that range"}

    row = rows[0]

    return {
        "found": True,
        "stn": stn,
        "wban": wban,
        "start_date": start_date,
        "end_date": end_date,
        "temperature": {
            "min_c": f_to_c(row["temp_min_f"]),
            "max_c": f_to_c(row["temp_max_f"]),
            "mean_c": f_to_c(row["temp_mean_f"]),
        },
        "rainfall": {
            "total_mm": inches_to_mm(row["prcp_sum_in"]),
        },
        "wind": {
            "mean_kmh": knots_to_kmh(row["wind_mean_knots"]),
        },
    }


@mcp.tool()
async def yearly_max_temp(stn: str, wban: str, year: int) -> dict:
    """
    Get the hottest day in a year: date and max temperature (°C).
    """

    query = f"""
    SELECT
      date,
      max AS max_temp_f
    FROM `{DATASET_TABLE}`
    WHERE stn = @stn
      AND wban = @wban
      AND EXTRACT(YEAR FROM date) = @year
      AND max != 9999.9
    ORDER BY max_temp_f DESC
    LIMIT 1
    """

    job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("stn", "STRING", stn),
                bigquery.ScalarQueryParameter("wban", "STRING", wban),
                bigquery.ScalarQueryParameter("year", "INT64", year),
            ]
        ),
    )
    rows = list(job)
    if not rows:
        return {"found": False, "reason": "No data for that year"}

    row = rows[0]
    return {
        "found": True,
        "stn": stn,
        "wban": wban,
        "year": year,
        "date": str(row["date"]),
        "max_c": f_to_c(row["max_temp_f"]),
    }


@mcp.tool()
async def daily_weather_series(
    stn: str,
    wban: str,
    start_date: str,
    end_date: str,
    metrics: Optional[List[str]] = None,
) -> dict:
    """
    Return daily time series for the given metrics between two dates.

    metrics can include: ["temp_mean_c", "temp_max_c", "temp_min_c", "rain_mm", "wind_kmh"].
    """

    if not metrics:
        metrics = ["temp_mean_c"]

    query = f"""
    SELECT
      date,
      temp,
      max,
      min,
      prcp,
      wdsp
    FROM `{DATASET_TABLE}`
    WHERE stn = @stn
      AND wban = @wban
      AND date BETWEEN @start_date AND @end_date
      AND temp != 9999.9
    ORDER BY date
    """

    job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("stn", "STRING", stn),
                bigquery.ScalarQueryParameter("wban", "STRING", wban),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        ),
    )

    series = []
    for row in job:
        item = {"date": str(row["date"])}
        if "temp_mean_c" in metrics:
            item["temp_mean_c"] = f_to_c(row["temp"])
        if "temp_max_c" in metrics and row["max"] != 9999.9:
            item["temp_max_c"] = f_to_c(row["max"])
        if "temp_min_c" in metrics and row["min"] != 9999.9:
            item["temp_min_c"] = f_to_c(row["min"])
        if "rain_mm" in metrics and row["prcp"] is not None and row["prcp"] < 99.99:
            item["rain_mm"] = inches_to_mm(row["prcp"])
        if "wind_kmh" in metrics and row["wdsp"] is not None and row["wdsp"] < 999.9:
            item["wind_kmh"] = knots_to_kmh(row["wdsp"])

        series.append(item)

    return {
        "stn": stn,
        "wban": wban,
        "start_date": start_date,
        "end_date": end_date,
        "metrics": metrics,
        "data": series,
    }


if __name__ == "__main__":
    # stdio transport so ToolHive can proxy it
    mcp.run()
