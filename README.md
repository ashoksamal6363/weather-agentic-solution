# Weather Agentic Solution (MCP + BigQuery + Llama Stack)

This repo provides:

- An MCP server (`mcp-weather-bq`) that reads **historical weather** from
  **NOAA GSOD public dataset on BigQuery**.
- Tools for:
  - `resolve_city` → city → station
  - `range_weather_summary` → min/max/mean temp + rain + wind for a date range
  - `yearly_max_temp` → hottest day in a year
  - `daily_weather_series` → daily time-series
- A Kubernetes `MCPServer` CR to deploy the MCP on **OpenShift AI** using **ToolHive**.
- The MCP can be registered in **Llama Stack** and used by agents for questions like:
  - “Compare Kuwait and Doha temperature in Jan 2025 and tell me which was hotter.”

## Quick customization points

You MUST:
- Provide a **GCP service account key** for BigQuery (as a Kubernetes Secret).
- Change the **container image name** in `mcp-weather-bq/k8s-mcpserver-weather-bq.yaml`
  to your own Quay.io repo.
