# Ivy.Data.BigQuery.NetTopologySuite

NetTopologySuite spatial support for the BigQuery ADO.NET provider.

## Installation

```bash
dotnet add package Ivy.Data.BigQuery.NetTopologySuite
```

## Usage

Call `UseNetTopologySuite()` once at application startup to enable NTS geometry support:

```csharp
using Ivy.Data.BigQuery;

// Enable NTS support (call once at startup)
BigQueryNtsExtensions.UseNetTopologySuite();

// Now you can use NTS Geometry types with BigQuery GEOGRAPHY columns
using var connection = new BigQueryConnection(connectionString);
await connection.OpenAsync();

var cmd = connection.CreateCommand();

// Reading geography data as NTS Geometry
cmd.CommandText = "SELECT location FROM places LIMIT 10";
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    var geometry = reader.GetFieldValue<Geometry>(0);
    Console.WriteLine($"Location: {geometry}");
}

// Using NTS Geometry as parameters
cmd.CommandText = "SELECT * FROM places WHERE ST_CONTAINS(@region, location)";
cmd.Parameters.Add(new BigQueryParameter("@region", myPolygon));
```

## Supported Types

This package enables mapping between BigQuery `GEOGRAPHY` columns and NetTopologySuite geometry types:

- `Point`
- `LineString`
- `Polygon`
- `MultiPoint`
- `MultiLineString`
- `MultiPolygon`
- `GeometryCollection`

## Custom Geometry Services

You can provide a custom `NtsGeometryServices` instance:

```csharp
var geometryServices = new NtsGeometryServices(
    new PrecisionModel(PrecisionModels.Floating),
    srid: 4326);

BigQueryNtsExtensions.UseNetTopologySuite(geometryServices);
```

## License

MIT
