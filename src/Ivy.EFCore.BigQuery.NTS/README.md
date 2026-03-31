# Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite

NetTopologySuite plugin for the BigQuery Entity Framework Core provider.

## Installation

```bash
dotnet add package Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite
```

## Usage

Call `UseNetTopologySuite()` when configuring your DbContext:

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseBigQuery(connectionString, o => o.UseNetTopologySuite()));
```

Now you can use NTS Geometry types with BigQuery `GEOGRAPHY` columns:

```csharp
public class Place
{
    public int Id { get; set; }
    public string Name { get; set; }
    public Point Location { get; set; }  // Maps to GEOGRAPHY
}
```

## Supported Translations

### Methods

| NTS Method | BigQuery Function |
|------------|-------------------|
| `Distance(Geometry)` | `ST_DISTANCE` |
| `Contains(Geometry)` | `ST_CONTAINS` |
| `Intersects(Geometry)` | `ST_INTERSECTS` |
| `Within(Geometry)` | `ST_WITHIN` |
| `Covers(Geometry)` | `ST_COVERS` |
| `CoveredBy(Geometry)` | `ST_COVEREDBY` |
| `Disjoint(Geometry)` | `ST_DISJOINT` |
| `Touches(Geometry)` | `ST_TOUCHES` |
| `EqualsTopologically(Geometry)` | `ST_EQUALS` |
| `IsWithinDistance(Geometry, double)` | `ST_DWITHIN` |
| `Buffer(double)` | `ST_BUFFER` |
| `ConvexHull()` | `ST_CONVEXHULL` |
| `Difference(Geometry)` | `ST_DIFFERENCE` |
| `Intersection(Geometry)` | `ST_INTERSECTION` |
| `Union(Geometry)` | `ST_UNION` |
| `SymmetricDifference(Geometry)` | Emulated via `ST_UNION`/`ST_DIFFERENCE` |
| `AsText()` | `ST_ASTEXT` |
| `AsBinary()` | `ST_ASBINARY` |
| `GetPointN(int)` | `ST_POINTN` |
| `GetGeometryN(int)` | `ST_DUMP` with array access |

### Properties

| NTS Property | BigQuery Function |
|--------------|-------------------|
| `Area` | `ST_AREA` |
| `Length` | `ST_LENGTH` |
| `Centroid` | `ST_CENTROID` |
| `Dimension` | `ST_DIMENSION` |
| `GeometryType` | `ST_GEOMETRYTYPE` |
| `IsEmpty` | `ST_ISEMPTY` |
| `NumGeometries` | `ST_NUMGEOMETRIES` |
| `NumPoints` | `ST_NUMPOINTS` |
| `Point.X` | `ST_X` |
| `Point.Y` | `ST_Y` |
| `LineString.StartPoint` | `ST_STARTPOINT` |
| `LineString.EndPoint` | `ST_ENDPOINT` |
| `LineString.IsClosed` | `ST_ISCLOSED` |
| `LineString.IsRing` | `ST_ISRING` |
| `Polygon.ExteriorRing` | `ST_EXTERIORRING` |
| `Polygon.NumInteriorRings` | `ARRAY_LENGTH(ST_INTERIORRINGS(...))` |