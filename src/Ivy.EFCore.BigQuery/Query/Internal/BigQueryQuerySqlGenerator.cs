using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal
{
    public class BigQueryQuerySqlGenerator : QuerySqlGenerator
    {
        private readonly ISqlGenerationHelper _sqlGenerationHelper;
        private readonly IRelationalTypeMappingSource _typeMappingSource;

        public BigQueryQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
        {
            _sqlGenerationHelper = dependencies.SqlGenerationHelper;
            _typeMappingSource = typeMappingSource;
        }

        protected override Expression VisitColumn(ColumnExpression columnExpression)
        {
            // In BQ, UNNEST values and WITH OFFSET columns are accessed directly
            // without table.column qualification

            // 1. UNNEST value column: emit as table alias only
            if (columnExpression.Name == "value")
            {
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(columnExpression.TableAlias));
                return columnExpression;
            }

            // 2. UNNEST WITH OFFSET column: emit as column name only
            if (columnExpression.Name == "offset")
            {
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
                return columnExpression;
            }

            return base.VisitColumn(columnExpression);
        }

        protected override Expression VisitSqlBinary(SqlBinaryExpression binary)
        {
            switch (binary.OperatorType)
            {
                case ExpressionType.Add:
                    {
                        if (binary.Type == typeof(string)
                            || binary.Left.TypeMapping?.ClrType == typeof(string)
                            || binary.Right.TypeMapping?.ClrType == typeof(string))
                        {
                            Visit(binary.Left);
                            Sql.Append(" || ");
                            Visit(binary.Right);
                            return binary;
                        }
                        return base.VisitSqlBinary(binary);
                    }
                default:
                    return base.VisitSqlBinary(binary);
            }
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            return extensionExpression switch
            {
                BigQueryUnnestExpression unnestExpression => VisitBigQueryUnnest(unnestExpression),
                BigQueryArrayIndexExpression arrayIndexExpression => VisitBigQueryArrayIndex(arrayIndexExpression),
                BigQueryArrayLiteralExpression arrayLiteralExpression => VisitBigQueryArrayLiteral(arrayLiteralExpression),
                BigQueryInUnnestExpression inUnnestExpression => VisitBigQueryInUnnest(inUnnestExpression),
                BigQueryStructAccessExpression structAccessExpression => VisitBigQueryStructAccess(structAccessExpression),
                //BigQueryArrayConstructorExpression arrayConstructorExpression => VisitBigQueryArrayConstructor(arrayConstructorExpression),
                BigQueryStructConstructorExpression structConstructorExpression => VisitBigQueryStructConstructor(structConstructorExpression),
                BigQueryJsonTraversalExpression jsonTraversalExpression => VisitBigQueryJsonTraversal(jsonTraversalExpression),
                BigQueryExtractExpression extractExpression => VisitBigQueryExtract(extractExpression),
                _ => base.VisitExtension(extensionExpression)
            };
        }

        protected virtual Expression VisitBigQueryExtract(BigQueryExtractExpression extractExpression)
        {
            Sql.Append("EXTRACT(");
            Sql.Append(extractExpression.Part);
            Sql.Append(" FROM ");
            Visit(extractExpression.Argument);
            Sql.Append(")");

            return extractExpression;
        }

        /// <summary>
        /// Generates SQL for JSON traversal using BigQuery's dot notation.
        /// e.g. `j`.`Customer`.Name
        /// </summary>
        protected virtual Expression VisitBigQueryJsonTraversal(BigQueryJsonTraversalExpression jsonTraversalExpression)
        {
            Visit(jsonTraversalExpression.Expression);
            foreach (var pathComponent in jsonTraversalExpression.Path)
            {
                if (pathComponent is SqlConstantExpression { Value: string fieldName })
                {
                    Sql.Append(".");
                    Sql.Append(fieldName);
                }
                else
                {
                    Sql.Append("[");
                    Visit(pathComponent);
                    Sql.Append("]");
                }
            }

            return jsonTraversalExpression;
        }

        protected virtual Expression VisitBigQueryUnnest(BigQueryUnnestExpression unnestExpression)
        {
            Sql.Append("UNNEST(");
            Visit(unnestExpression.Array);
            Sql.Append(")");

            if (!string.IsNullOrEmpty(unnestExpression.Alias))
            {
                Sql.Append(AliasSeparator);
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(unnestExpression.Alias));
            }

            if (unnestExpression.WithOffset)
            {
                Sql.Append(" WITH OFFSET");
                if (!string.IsNullOrEmpty(unnestExpression.OffsetAlias))
                {
                    Sql.Append(AliasSeparator);
                    Sql.Append(_sqlGenerationHelper.DelimitIdentifier(unnestExpression.OffsetAlias));
                }
            }

            return unnestExpression;
        }

        protected virtual Expression VisitBigQueryArrayIndex(BigQueryArrayIndexExpression arrayIndexExpression)
        {
            Visit(arrayIndexExpression.Array);
            Sql.Append("[OFFSET(");
            Visit(arrayIndexExpression.Index);
            Sql.Append(")]");

            return arrayIndexExpression;
        }

        protected virtual Expression VisitBigQueryInUnnest(BigQueryInUnnestExpression inUnnestExpression)
        {
            Visit(inUnnestExpression.Item);
            Sql.Append(" IN UNNEST(");
            Visit(inUnnestExpression.Array);
            Sql.Append(")");

            return inUnnestExpression;
        }

        protected virtual Expression VisitBigQueryArrayLiteral(BigQueryArrayLiteralExpression arrayLiteralExpression)
        {
            Sql.Append("ARRAY<");
            Sql.Append(arrayLiteralExpression.ElementTypeMapping?.StoreType ?? "INT64");
            Sql.Append(">[");

            for (int i = 0; i < arrayLiteralExpression.Elements.Count; i++)
            {
                if (i > 0)
                {
                    Sql.Append(", ");
                }
                Visit(arrayLiteralExpression.Elements[i]);
            }

            Sql.Append("]");
            return arrayLiteralExpression;
        }

        protected virtual Expression VisitBigQueryStructAccess(BigQueryStructAccessExpression structAccessExpression)
        {
            Visit(structAccessExpression.Struct);
            Sql.Append(".");
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(structAccessExpression.FieldName));

            return structAccessExpression;
        }

            //protected virtual Expression VisitBigQueryArrayConstructor(BigQueryArrayConstructorExpression arrayConstructorExpression)
            //{
            //    if (arrayConstructorExpression.UseArrayKeyword)
            //    {
            //        Sql.Append("ARRAY");
            //        if (!string.IsNullOrEmpty(arrayConstructorExpression.ExplicitType))
            //        {
            //            Sql.Append("<").Append(arrayConstructorExpression.ExplicitType).Append(">");
            //        }
            //    }

            //    Sql.Append("[");

            //    for (var i = 0; i < arrayConstructorExpression.Elements.Count; i++)
            //    {
            //        if (i > 0)
            //        {
            //            Sql.Append(", ");
            //        }
            //        Visit(arrayConstructorExpression.Elements[i]);
            //    }

            //    Sql.Append("]");

            //    return arrayConstructorExpression;
            //}

        protected virtual Expression VisitBigQueryStructConstructor(BigQueryStructConstructorExpression structConstructorExpression)
        {
            Sql.Append("STRUCT");

            if (!string.IsNullOrEmpty(structConstructorExpression.ExplicitType))
            {
                Sql.Append("<").Append(structConstructorExpression.ExplicitType).Append(">");
            }

            Sql.Append("(");

            for (var i = 0; i < structConstructorExpression.Arguments.Count; i++)
            {
                if (i > 0)
                {
                    Sql.Append(", ");
                }

                Visit(structConstructorExpression.Arguments[i]);

                // Add field name alias if provided
                if (structConstructorExpression.FieldNames != null
                    && i < structConstructorExpression.FieldNames.Count
                    && !string.IsNullOrEmpty(structConstructorExpression.FieldNames[i]))
                {
                    Sql.Append(AliasSeparator).Append(_sqlGenerationHelper.DelimitIdentifier(structConstructorExpression.FieldNames[i]));
                }
            }

            Sql.Append(")");

            return structConstructorExpression;
        }

        protected override void GenerateTop(SelectExpression selectExpression)
        {
            // BigQuery does not support TOP; use LIMIT instead
        }

        protected override void GenerateLimitOffset(SelectExpression selectExpression)
        {
            if (selectExpression.Limit != null)
            {
                Sql.AppendLine().Append("LIMIT ");
                Visit(selectExpression.Limit);
            }

            if (selectExpression.Offset != null)
            {
                if (selectExpression.Limit == null)
                {
                    Sql.AppendLine();
                }
                else
                {
                    Sql.Append(" ");
                }

                Sql.Append("OFFSET ");
                Visit(selectExpression.Offset);
            }
        }

        protected override Expression VisitJsonScalar(JsonScalarExpression jsonScalarExpression)
        {
            var path = jsonScalarExpression.Path;
            if (path.Count == 0)
            {
                Visit(jsonScalarExpression.Json);
                return jsonScalarExpression;
            }

            // JSON_EXTRACT for objects/arrays, JSON_EXTRACT_SCALAR for primitives
            var isScalar = jsonScalarExpression.TypeMapping is not Storage.Internal.Mapping.BigQueryOwnedJsonTypeMapping
                && jsonScalarExpression.TypeMapping?.ElementTypeMapping is null;

            if (isScalar)
            {
                // JSON_EXTRACT_SCALAR always returns STRING in BQ
                // We need to cast to the appropriate type for non-string types
                var castType = GetBigQueryCastType(jsonScalarExpression.Type);
                if (castType != null)
                {
                    Sql.Append("CAST(");
                }

                Sql.Append("JSON_EXTRACT_SCALAR(");
                Visit(jsonScalarExpression.Json);
                Sql.Append(", ");
                GenerateJsonPath(path);
                Sql.Append(")");

                if (castType != null)
                {
                    Sql.Append(" AS ").Append(castType).Append(")");
                }
            }
            else
            {
                Sql.Append("JSON_EXTRACT(");
                Visit(jsonScalarExpression.Json);
                Sql.Append(", ");
                GenerateJsonPath(path);
                Sql.Append(")");
            }

            return jsonScalarExpression;
        }

        private static string? GetBigQueryCastType(Type clrType)
        {
            var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

            if (underlyingType.IsEnum)
            {
                underlyingType = Enum.GetUnderlyingType(underlyingType);
            }

            return underlyingType switch
            {
                Type t when t == typeof(int) => "INT64",
                Type t when t == typeof(long) => "INT64",
                Type t when t == typeof(short) => "INT64",
                Type t when t == typeof(byte) => "INT64",
                Type t when t == typeof(sbyte) => "INT64",
                Type t when t == typeof(uint) => "INT64",
                Type t when t == typeof(ulong) => "BIGNUMERIC",
                Type t when t == typeof(ushort) => "INT64",
                Type t when t == typeof(double) => "FLOAT64",
                Type t when t == typeof(float) => "FLOAT64",
                Type t when t == typeof(decimal) => "BIGNUMERIC",
                Type t when t == typeof(bool) => "BOOL",
                Type t when t == typeof(string) => null,
                Type t when t == typeof(DateTime) => "DATETIME",
                Type t when t == typeof(DateOnly) => "DATE",
                Type t when t == typeof(TimeOnly) => "TIME",
                Type t when t == typeof(TimeSpan) => "TIME",
                Type t when t == typeof(Guid) => null,
                _ => null
            };
        }

        private void GenerateJsonPath(IReadOnlyList<PathSegment> path)
        {
            Sql.Append("'$");

            foreach (var pathSegment in path)
            {
                switch (pathSegment)
                {
                    case { PropertyName: string propertyName }:
                        Sql.Append(".").Append(propertyName);
                        break;

                    case { ArrayIndex: SqlConstantExpression { Value: int index } }:
                        Sql.Append("[").Append(index.ToString()).Append("]");
                        break;

                    case { ArrayIndex: not null }:
                        // Dynamic array index - BigQuery doesn't support this in JSON path strings
                        throw new InvalidOperationException(
                            "BigQuery JSON paths do not support dynamic array indices. " +
                            "Array indices must be constant values.");

                    default:
                        throw new InvalidOperationException($"Unknown path segment type: {pathSegment}");
                }
            }

            Sql.Append("'");
        }

        protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
        {
            Sql.Append("CROSS JOIN ");
            Visit(crossApplyExpression.Table);

            return crossApplyExpression;
        }

        protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
        {
            Sql.Append("LEFT JOIN ");
            Visit(outerApplyExpression.Table);
            Sql.Append(" ON TRUE");

            return outerApplyExpression;
        }

        protected override string GetOperator(SqlBinaryExpression e)
        {
            return e.OperatorType switch
            {
                ExpressionType.Add when e.Type == typeof(string)
                    || e.Left.TypeMapping?.ClrType == typeof(string)
                    || e.Right.TypeMapping?.ClrType == typeof(string) =>
                    throw new InvalidOperationException("BigQuery does not support the '+' operator for string concatenation. Use the CONCAT function instead."),

                // BigQuery uses || for logical OR, but we need to distinguish from string concatenation
                ExpressionType.OrElse => " OR ",
                ExpressionType.AndAlso => " AND ",

                // Standard operators
                _ => base.GetOperator(e)
            };
        }

        protected override bool RequiresParentheses(SqlExpression outerExpression, SqlExpression innerExpression)
        {
            // BigQuery-specific precedence rules
            if (TryGetOperatorInfo(outerExpression, out var outerPrecedence, out var isOuterAssociative)
                && TryGetOperatorInfo(innerExpression, out var innerPrecedence, out _))
            {
                return outerPrecedence.CompareTo(innerPrecedence) switch
                {
                    > 0 => true,
                    < 0 => false,
                    _ => outerExpression is not SqlBinaryExpression outerBinary
                        || innerExpression is not SqlBinaryExpression innerBinary
                        || outerBinary.OperatorType != innerBinary.OperatorType
                        || !isOuterAssociative
                };
            }

            return base.RequiresParentheses(outerExpression, innerExpression);
        }

        //https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#operator_precedence
        protected override bool TryGetOperatorInfo(SqlExpression expression, out int precedence, out bool isAssociative)
        {
            (precedence, isAssociative) = expression switch
            {
                SqlBinaryExpression binary => binary.OperatorType switch
                {
                    ExpressionType.Multiply => (1400, true),
                    ExpressionType.Divide => (1400, false),
                    ExpressionType.Modulo => (1400, false),

                    ExpressionType.Add => (1300, true),
                    ExpressionType.Subtract => (1300, false),

                    ExpressionType.LeftShift => (1200, false),
                    ExpressionType.RightShift => (1200, false),

                    ExpressionType.And when binary.Type != typeof(bool) => (1100, true),

                    ExpressionType.ExclusiveOr => (1000, true),

                    ExpressionType.Or when binary.Type != typeof(bool) => (900, true),

                    ExpressionType.Equal => (800, false),
                    ExpressionType.NotEqual => (800, false),
                    ExpressionType.LessThan => (800, false),
                    ExpressionType.LessThanOrEqual => (800, false),
                    ExpressionType.GreaterThan => (800, false),
                    ExpressionType.GreaterThanOrEqual => (800, false),

                    ExpressionType.Not when binary.Type == typeof(bool) => (700, false),

                    ExpressionType.AndAlso => (600, true),
                    ExpressionType.And when binary.Type == typeof(bool) => (600, true),

                    ExpressionType.OrElse => (500, true),
                    ExpressionType.Or when binary.Type == typeof(bool) => (500, true),

                    _ => default
                },

                SqlUnaryExpression unary => unary.OperatorType switch
                {
                    ExpressionType.Convert => (1600, false),
                    ExpressionType.Negate => (1500, false),
                    ExpressionType.Not => (700, false),
                    ExpressionType.Equal => (800, false), // IS NULL
                    ExpressionType.NotEqual => (800, false), // IS NOT NULL
                    _ => default
                },

                //BigQueryArrayAccessExpression => (1600, false),
                BigQueryStructAccessExpression => (1600, false),

                _ => default
            };

            return precedence != default;
        }

        protected override Expression VisitUpdate(UpdateExpression updateExpression)
        {
            var selectExpression = updateExpression.SelectExpression;

            if (selectExpression is
                {
                    GroupBy: [],
                    Having: null,
                    Projection: [],
                    Orderings: [],
                    Offset: null
                })
            {
                Sql.Append("UPDATE ");
                Visit(updateExpression.Table);
                Sql.AppendLine();
                Sql.Append("SET ");
                Sql.Append(
                $"{_sqlGenerationHelper.DelimitIdentifier(updateExpression.ColumnValueSetters[0].Column.Name)} = ");
                Visit(updateExpression.ColumnValueSetters[0].Value);
                using (Sql.Indent())
                {
                    foreach (var columnValueSetter in updateExpression.ColumnValueSetters.Skip(1))
                    {
                        Sql.AppendLine(",");
                        Sql.Append($"{_sqlGenerationHelper.DelimitIdentifier(columnValueSetter.Column.Name)} = ");
                        Visit(columnValueSetter.Value);
                    }
                }

                var predicate = selectExpression.Predicate;
                if (selectExpression.Tables.Count > 1)
                {
                    Sql.AppendLine().Append("FROM ");

                    var tablesToJoin = selectExpression.Tables
                        .Where(t => (t as JoinExpressionBase)?.Table.Alias != updateExpression.Table.Alias && t.Alias != updateExpression.Table.Alias)
                        .ToList();

                    for (var i = 0; i < tablesToJoin.Count; i++)
                    {
                        var tableToJoin = tablesToJoin[i];
                        if (i > 0)
                        {
                            Sql.AppendLine(", ");
                        }
                        Visit(tableToJoin);
                    }

                    foreach (var table in selectExpression.Tables)
                    {
                        if (table is PredicateJoinExpressionBase predicateJoinExpression)
                        {
                            predicate = predicate == null
                                ? predicateJoinExpression.JoinPredicate
                                : new SqlBinaryExpression(
                                    ExpressionType.AndAlso,
                                    predicate,
                                    predicateJoinExpression.JoinPredicate,
                                    typeof(bool),
                                    predicate.TypeMapping);
                        }
                    }
                }

                if (selectExpression.Predicate != null)
                {
                    Sql.AppendLine().Append("WHERE ");
                    Visit(selectExpression.Predicate);
                }
                else
                {
                    // WHERE is always required
                    Sql.AppendLine().Append("WHERE true");
                }

                return updateExpression;
            }

            throw new InvalidOperationException(
                RelationalStrings.ExecuteOperationWithUnsupportedOperatorInSqlGeneration(nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate)));
        }

        protected override Expression VisitDelete(DeleteExpression deleteExpression)
        {
            var selectExpression = deleteExpression.SelectExpression;

            if (selectExpression is
                {
                    Tables: [var table],
                    GroupBy: [],
                    Having: null,
                    Projection: [],
                    Orderings: [],
                    Offset: null,
                    Limit: null
                }
                && table.Equals(deleteExpression.Table))
            {
                Sql.Append("DELETE FROM ");
                Visit(deleteExpression.Table);

                if (selectExpression.Predicate != null)
                {
                    Sql.AppendLine().Append("WHERE ");
                    Visit(selectExpression.Predicate);
                }
                else
                {
                    // WHERE is always required
                    Sql.AppendLine().Append("WHERE true");
                }

                return deleteExpression;
            }

            throw new InvalidOperationException(
                RelationalStrings.ExecuteOperationWithUnsupportedOperatorInSqlGeneration(
                    nameof(EntityFrameworkQueryableExtensions.ExecuteDelete)));
        }

        protected override void GeneratePseudoFromClause()
        {
        }

        protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
        {
            // BigQuery doesn't allow parameterized types in CAST expressions
            // CAST(x AS BIGNUMERIC(57,28)) -> CAST(x AS BIGNUMERIC)
            if (sqlUnaryExpression.OperatorType == ExpressionType.Convert)
            {
                Sql.Append("CAST(");
                Visit(sqlUnaryExpression.Operand);
                Sql.Append(" AS ");

                var storeType = sqlUnaryExpression.TypeMapping?.StoreType;
                if (storeType != null)
                {
                    // Strip precision/scale from numeric types for CAST expressions
                    var openParen = storeType.IndexOf('(');
                    if (openParen > 0 && (storeType.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase)
                                        || storeType.StartsWith("BIGNUMERIC", StringComparison.OrdinalIgnoreCase)))
                    {
                        storeType = storeType.Substring(0, openParen);
                    }
                    Sql.Append(storeType);
                }

                Sql.Append(")");
                return sqlUnaryExpression;
            }

            return base.VisitSqlUnary(sqlUnaryExpression);
        }
    }
}