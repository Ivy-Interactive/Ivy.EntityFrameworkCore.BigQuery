# Correlated Subqueries in BigQuery

BigQuery has limited support for correlated subqueries compared to databases like SQL Server or PostgreSQL. This document explains which LINQ patterns are supported and which ones to avoid.

## Background

A correlated subquery is a subquery that references columns from the outer query. For example, when you project a value from a navigation property, EF Core generates a correlated scalar subquery:

```csharp
// This LINQ:
customers.Select(c => c.Orders.FirstOrDefault().OrderDate)

// Generates SQL like:
SELECT (SELECT o.OrderDate FROM Orders o WHERE c.CustomerID = o.CustomerID LIMIT 1)
FROM Customers c
```

BigQuery rejects this with: *"Correlated subqueries that reference other tables are not supported unless they can be de-correlated"*

## Supported Patterns

The provider automatically rewrites the following patterns to work with BigQuery:

### Scalar projections from navigation properties

```csharp
// First/FirstOrDefault
customers.Select(c => new {
    c.CustomerID,
    FirstOrderDate = c.Orders.OrderBy(o => o.OrderID).FirstOrDefault().OrderDate
})

// Single/SingleOrDefault, Last/LastOrDefault also supported
```

### Aggregate projections

```csharp
// Count
customers.Select(c => new { c.CustomerID, OrderCount = c.Orders.Count() })

// Sum, Average, Min, Max
customers.Select(c => new { c.CustomerID, Total = c.Orders.Sum(o => o.Total) })

// With filters
customers.Select(c => new {
    c.CustomerID,
    RecentOrderCount = c.Orders.Count(o => o.OrderDate > someDate)
})
```

### How it works

The provider transforms correlated scalar subqueries into LEFT JOINs:

```sql
-- Transformed query for FirstOrDefault:
SELECT s._scalar_value
FROM Customers c
LEFT JOIN (
    SELECT o.OrderDate AS _scalar_value,
           o.CustomerID AS _partition0,
           ROW_NUMBER() OVER(PARTITION BY o.CustomerID ORDER BY o.OrderID) AS rn
    FROM Orders o
) AS s ON c.CustomerID = s._partition0 AND s.rn = 1

-- Transformed query for Count:
SELECT COALESCE(s._scalar_value, 0)
FROM Customers c
LEFT JOIN (
    SELECT COUNT(*) AS _scalar_value, o.CustomerID AS _partition0
    FROM Orders o
    GROUP BY o.CustomerID
) AS s ON c.CustomerID = s._partition0
```

### SelectMany with correlated predicates

```csharp
// Correlated SelectMany
from c in customers
from o in orders.Where(o => o.CustomerID == c.CustomerID)
select new { c.CustomerID, o.OrderID }

// With additional joins
from g in gears
from t in tags
join g2 in gears on g.FullName equals g2.Nickname  // Both reference outer 'g'
select new { g, t, g2 }
```

These patterns generate OUTER APPLY or CROSS APPLY, which BigQuery doesn't support.
The provider transforms them to LEFT/INNER JOINs by:
- Extracting correlated predicates from inner subqueries to outer JOIN ON clauses
- Removing correlated projections and remapping outer references
- Handling "both-sides-outer" correlations where both sides reference ancestor tables

## Unsupported Patterns

### Take/Skip with correlated subqueries

Queries that use `Take()` or `Skip()` inside correlated collection projections:

```csharp
// NOT SUPPORTED - Take inside correlated collection projection
customers.Select(c => new {
    c.CustomerID,
    TopOrders = c.Orders.Take(5).ToList()
})

// NOT SUPPORTED - Take in collection projection with FirstOrDefault on top
customers.Take(10).Select(c => new {
    c.CustomerID,
    FirstOrder = c.Orders.Take(2).FirstOrDefault()
})
```

The provider cannot transform LIMIT/OFFSET combined with correlated predicates.

**Workaround:** Use explicit aggregation with ROW_NUMBER:

```csharp
// Load all orders and filter in memory
var orders = context.Orders.ToList();
var customers = context.Customers
    .Select(c => new {
        c.CustomerID,
        TopOrders = orders.Where(o => o.CustomerID == c.CustomerID).Take(5).ToList()
    });
```

### Deeply nested correlated subqueries

Queries where an inner subquery references a table from multiple levels up:

```csharp
// NOT SUPPORTED - inner subquery references 'c' from two levels up
customers.Select(c => new {
    c.CustomerID,
    Value = c.Orders
        .Select(o => o.OrderDetails
            .Count(od => od.ProductID > c.CustomerID.Length))  // References 'c'
        .FirstOrDefault()
})

// NOT SUPPORTED - chained navigation with FirstOrDefault
customers.Select(c => c.Orders.FirstOrDefault().OrderDetails.FirstOrDefault())
```

### EXISTS/IN subqueries in JOIN predicates

BigQuery doesn't support EXISTS or IN subqueries inside JOIN ON clauses:

```csharp
// NOT SUPPORTED - EXISTS in JOIN predicate
from g in gears
join l in locustLeaders on !locustLeaders.Any(x => x.ThreatLevel == g.ThreatLevel)
select g
```

This generates SQL like `JOIN ... ON NOT EXISTS (...)` which BigQuery rejects.

### Correlated subqueries in WHERE clauses

Some patterns may not be automatically transformed:

```csharp
// May not work
customers.Where(c => c.Orders.Any(o => o.Total > 1000))
```

**Workaround:** Use explicit joins:

```csharp
// Use join instead
from c in customers
join o in orders on c.CustomerID equals o.CustomerID into customerOrders
where customerOrders.Any(o => o.Total > 1000)
select c
```

## Troubleshooting

If you encounter the error *"Correlated subqueries that reference other tables are not supported"*, try:

1. **Simplify the query** - Break complex projections into multiple queries
2. **Use explicit joins** - Rewrite using `join` syntax instead of navigation properties
3. **Load related data separately** - Use `Include()` or separate queries