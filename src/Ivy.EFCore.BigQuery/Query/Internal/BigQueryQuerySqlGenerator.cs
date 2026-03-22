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

        // Columns from JSON UNNEST tables need JSON_VALUE extraction
        private readonly HashSet<string> _jsonUnnestAliases = new();

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

            // 3. Column from JSON UNNEST table: generate JSON_VALUE extraction
            if (_jsonUnnestAliases.Contains(columnExpression.TableAlias))
            {                
                var isScalar = IsScalarType(columnExpression.Type);

                var castType = isScalar ? GetBigQueryCastType(columnExpression.Type) : null;

                if (castType != null)
                {
                    Sql.Append("CAST(");
                }

                Sql.Append(isScalar ? "JSON_VALUE" : "JSON_QUERY");
                Sql.Append("(");
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(columnExpression.TableAlias));
                Sql.Append(", '$.");
                Sql.Append(columnExpression.Name);
                Sql.Append("')");

                if (castType != null)
                {
                    Sql.Append(" AS ");
                    Sql.Append(castType);
                    Sql.Append(")");
                }

                return columnExpression;
            }

            return base.VisitColumn(columnExpression);
        }

        private static bool IsScalarType(Type type)
        {
            var unwrapped = Nullable.GetUnderlyingType(type) ?? type;
            return unwrapped.IsPrimitive
                   || unwrapped == typeof(string)
                   || unwrapped == typeof(decimal)
                   || unwrapped == typeof(DateTime)
                   || unwrapped == typeof(DateTimeOffset)
                   || unwrapped == typeof(DateOnly)
                   || unwrapped == typeof(TimeOnly)
                   || unwrapped == typeof(TimeSpan)
                   || unwrapped == typeof(Guid)
                   || unwrapped.IsEnum;
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
                case ExpressionType.Modulo:
                    {
                        Sql.Append("MOD(");
                        Visit(binary.Left);
                        Sql.Append(", ");
                        Visit(binary.Right);
                        Sql.Append(")");
                        return binary;
                    }
                // BigQuery doesn't allow bitwise operations with literal NULL
                // expr & NULL, expr | NULL, expr ^ NULL all result in NULL
                case ExpressionType.And:
                case ExpressionType.Or:
                    {
                        // Check if this is a bitwise operation (not boolean AND/OR)
                        if (binary.Type != typeof(bool))
                        {
                            var leftIsNull = IsNullConstant(binary.Left);
                            var rightIsNull = IsNullConstant(binary.Right);

                            if (leftIsNull || rightIsNull)
                            {
                                // Bitwise op with NULL returns NULL
                                Sql.Append("NULL");
                                return binary;
                            }
                        }
                        return base.VisitSqlBinary(binary);
                    }
                // Convert: A XOR B -> (A AND NOT B) OR (NOT A AND B)
                case ExpressionType.ExclusiveOr:
                    {
                        if (binary.Type == typeof(bool))
                        {
                            // Boolean XOR: (A AND NOT B) OR (NOT A AND B)
                            Sql.Append("((");
                            Visit(binary.Left);
                            Sql.Append(" AND NOT ");
                            Visit(binary.Right);
                            Sql.Append(") OR (NOT ");
                            Visit(binary.Left);
                            Sql.Append(" AND ");
                            Visit(binary.Right);
                            Sql.Append("))");
                            return binary;
                        }

                        // Bitwise XOR with NULL
                        var leftIsNull = IsNullConstant(binary.Left);
                        var rightIsNull = IsNullConstant(binary.Right);

                        if (leftIsNull || rightIsNull)
                        {
                            Sql.Append("NULL");
                            return binary;
                        }

                        return base.VisitSqlBinary(binary);
                    }
                // BQ doesn't allow comparison operators with literal NULL
                // NULL > x, x > NULL, etc. are not allowed - use FALSE instead
                // This also applies to expressions that will evaluate to NULL (e.g., bitwise ops with NULL)
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    {
                        var leftIsNull = WillEvaluateToNull(binary.Left);
                        var rightIsNull = WillEvaluateToNull(binary.Right);

                        if (leftIsNull || rightIsNull)
                        {
                            // Comparison with NULL is always unknown/false
                            Sql.Append("FALSE");
                            return binary;
                        }
                        return base.VisitSqlBinary(binary);
                    }
                default:
                    return base.VisitSqlBinary(binary);
            }
        }

        private static bool IsNullConstant(SqlExpression expression)
        {
            return expression is SqlConstantExpression { Value: null };
        }

        /// <summary>
        /// Checks if an expression will evaluate to NULL at runtime.
        /// This includes direct NULL constants and bitwise operations with NULL operands.
        /// </summary>
        private static bool WillEvaluateToNull(SqlExpression expression)
        {
            // Direct NULL constant
            if (expression is SqlConstantExpression { Value: null })
            {
                return true;
            }

            // Bitwise operation with NULL operand will evaluate to NULL
            if (expression is SqlBinaryExpression { OperatorType: var op } binary
                && op is ExpressionType.And or ExpressionType.Or or ExpressionType.ExclusiveOr
                && binary.Type != typeof(bool))
            {
                return IsNullConstant(binary.Left) || IsNullConstant(binary.Right);
            }

            return false;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            return extensionExpression switch
            {
                BigQueryUnnestExpression unnestExpression => VisitBigQueryUnnest(unnestExpression),
                BigQueryJsonUnnestExpression jsonUnnestExpression => VisitBigQueryJsonUnnest(jsonUnnestExpression),
                BigQueryJsonElementAccessExpression jsonElementAccess => VisitBigQueryJsonElementAccess(jsonElementAccess),
                BigQueryArrayIndexExpression arrayIndexExpression => VisitBigQueryArrayIndex(arrayIndexExpression),
                BigQueryArrayLiteralExpression arrayLiteralExpression => VisitBigQueryArrayLiteral(arrayLiteralExpression),
                BigQueryInUnnestExpression inUnnestExpression => VisitBigQueryInUnnest(inUnnestExpression),
                BigQueryStructAccessExpression structAccessExpression => VisitBigQueryStructAccess(structAccessExpression),
                BigQueryInlineArrayExpression inlineArrayExpression => VisitBigQueryInlineArray(inlineArrayExpression),
                BigQueryStructConstructorExpression structConstructorExpression => VisitBigQueryStructConstructor(structConstructorExpression),
                BigQueryJsonTraversalExpression jsonTraversalExpression => VisitBigQueryJsonTraversal(jsonTraversalExpression),
                BigQueryExtractExpression extractExpression => VisitBigQueryExtract(extractExpression),
                BigQueryIntervalExpression intervalExpression => VisitBigQueryInterval(intervalExpression),
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

        protected virtual Expression VisitBigQueryInterval(BigQueryIntervalExpression intervalExpression)
        {
            Sql.Append("INTERVAL ");
            Visit(intervalExpression.Value);
            Sql.Append(" ");
            Sql.Append(intervalExpression.DatePart);

            return intervalExpression;
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

        /// <summary>
        /// Generates SQL for JSON array UNNEST expressions.
        /// This handles UNNEST(JSON_QUERY_ARRAY(...)) for expanding JSON arrays into rows.
        /// </summary>
        protected virtual Expression VisitBigQueryJsonUnnest(BigQueryJsonUnnestExpression jsonUnnestExpression)
        {
            if (!string.IsNullOrEmpty(jsonUnnestExpression.Alias))
            {
                _jsonUnnestAliases.Add(jsonUnnestExpression.Alias);
            }

            Sql.Append("UNNEST(");
            Visit(jsonUnnestExpression.JsonArrayExpression);
            Sql.Append(")");

            if (!string.IsNullOrEmpty(jsonUnnestExpression.Alias))
            {
                Sql.Append(AliasSeparator);
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(jsonUnnestExpression.Alias));
            }

            if (jsonUnnestExpression.WithOffset)
            {
                Sql.Append(" WITH OFFSET");
                if (!string.IsNullOrEmpty(jsonUnnestExpression.OffsetAlias))
                {
                    Sql.Append(AliasSeparator);
                    Sql.Append(_sqlGenerationHelper.DelimitIdentifier(jsonUnnestExpression.OffsetAlias));
                }
            }

            return jsonUnnestExpression;
        }

        /// <summary>
        /// Generates SQL for JSON element property access.
        /// This outputs JSON_VALUE(element, '$.property') or JSON_QUERY(element, '$.property').
        /// </summary>
        protected virtual Expression VisitBigQueryJsonElementAccess(BigQueryJsonElementAccessExpression expression)
        {
            var functionName = expression.IsScalar ? "JSON_VALUE" : "JSON_QUERY";

            var castType = expression.IsScalar ? GetBigQueryCastType(expression.Type) : null;

            if (castType != null)
            {
                Sql.Append("CAST(");
            }

            Sql.Append(functionName);
            Sql.Append("(");
            Visit(expression.JsonElement);
            Sql.Append(", '$.");
            Sql.Append(expression.PropertyName);
            Sql.Append("')");

            if (castType != null)
            {
                Sql.Append(" AS ");
                Sql.Append(castType);
                Sql.Append(")");
            }

            return expression;
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

        protected virtual Expression VisitBigQueryInlineArray(BigQueryInlineArrayExpression inlineArrayExpression)
        {
            Sql.Append("[");

            for (var i = 0; i < inlineArrayExpression.Elements.Count; i++)
            {
                if (i > 0)
                {
                    Sql.Append(", ");
                }
                Visit(inlineArrayExpression.Elements[i]);
            }

            Sql.Append("]");

            return inlineArrayExpression;
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
            else if (selectExpression.Offset != null)
            {
                // BigQuery requires LIMIT when OFFSET is used
                // Use max INT64 value
                Sql.AppendLine().Append("LIMIT 9223372036854775807");
            }

            if (selectExpression.Offset != null)
            {
                Sql.Append(" OFFSET ");
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

            // JSON_QUERY for objects/arrays, JSON_VALUE for primitives
            var isScalar = jsonScalarExpression.TypeMapping is not Storage.Internal.Mapping.BigQueryOwnedJsonTypeMapping
                && jsonScalarExpression.TypeMapping?.ElementTypeMapping is null;

            if (isScalar)
            {
                // JSON_VALUE always returns STRING in BQ
                // We need to cast to the appropriate type for non-string types
                var castType = GetBigQueryCastType(jsonScalarExpression.Type);
                if (castType != null)
                {
                    Sql.Append("CAST(");
                }

                Sql.Append("JSON_VALUE(");
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
                Sql.Append("JSON_QUERY(");
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
                Type t when t == typeof(TimeSpan) => "INT64", // Microseconds
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
                        // For JoinExpressionBase, visit only the inner table, not the join itself
                        // The join predicate is handled separately below
                        if (tableToJoin is JoinExpressionBase joinExpression)
                        {
                            Visit(joinExpression.Table);
                        }
                        else
                        {
                            Visit(tableToJoin);
                        }
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

                if (predicate != null)
                {
                    Sql.AppendLine().Append("WHERE ");
                    Visit(predicate);
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

        protected override void GenerateSetOperation(SetOperationBase setOperation)
        {
            // BigQuery requires explicit ALL or DISTINCT for set operations
            // e.g., UNION DISTINCT, UNION ALL, EXCEPT DISTINCT, EXCEPT ALL
            GenerateSetOperationOperand(setOperation, setOperation.Source1);
            Sql.AppendLine()
                .Append(GetSetOperation(setOperation))
                .AppendLine(setOperation.IsDistinct ? " DISTINCT" : " ALL");
            GenerateSetOperationOperand(setOperation, setOperation.Source2);

            static string GetSetOperation(SetOperationBase operation)
                => operation switch
                {
                    ExceptExpression => "EXCEPT",
                    IntersectExpression => "INTERSECT",
                    UnionExpression => "UNION",
                    _ => throw new InvalidOperationException($"Unknown set operation type: {operation.GetType().Name}")
                };
        }

        /// <summary>
        /// BigQuery uses COLLATE(expression, 'collation') function syntax instead of
        /// the standard SQL "expression COLLATE collation" syntax.
        /// </summary>
        protected override Expression VisitCollate(CollateExpression collateExpression)
        {
            Sql.Append("COLLATE(");
            Visit(collateExpression.Operand);
            Sql.Append(", '");
            Sql.Append(collateExpression.Collation);
            Sql.Append("')");

            return collateExpression;
        }

        /// <summary>
        /// BQ requires DISTINCT without parentheses inside aggregate functions.
        /// When DISTINCT is applied to a CAST expression, BQ requires:
        /// DISTINCT CAST(x AS type), not CAST(DISTINCT x AS type).
        /// </summary>
        protected override Expression VisitDistinct(DistinctExpression distinctExpression)
        {
            Sql.Append("DISTINCT ");
            Visit(distinctExpression.Operand);
            return distinctExpression;
        }

        /// <summary>
        /// Override CAST handling for BigQuery-specific requirements:
        /// 1. Strip precision/scale from NUMERIC/BIGNUMERIC types
        /// 2. When CAST wraps a DISTINCT expression, reorder to: DISTINCT CAST(x AS type)
        /// </summary>
        protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
        {
            if (sqlUnaryExpression.OperatorType == ExpressionType.Convert)
            {
                if (sqlUnaryExpression.Operand is DistinctExpression distinctExpression)
                {
                    Sql.Append("DISTINCT CAST(");
                    Visit(distinctExpression.Operand);
                    Sql.Append(" AS ");
                    AppendStoreType(sqlUnaryExpression.TypeMapping?.StoreType);
                    Sql.Append(")");
                    return sqlUnaryExpression;
                }

                // Normal CAST handling
                Sql.Append("CAST(");
                Visit(sqlUnaryExpression.Operand);
                Sql.Append(" AS ");
                AppendStoreType(sqlUnaryExpression.TypeMapping?.StoreType);
                Sql.Append(")");
                return sqlUnaryExpression;
            }

            return base.VisitSqlUnary(sqlUnaryExpression);
        }

        private void AppendStoreType(string? storeType)
        {
            if (storeType != null)
            {
                // Strip precision/scale from numeric types for CAST expressions
                // BQ doesn't allow: CAST(x AS BIGNUMERIC(57,28))
                var openParen = storeType.IndexOf('(');
                if (openParen > 0 && (storeType.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase)
                                    || storeType.StartsWith("BIGNUMERIC", StringComparison.OrdinalIgnoreCase)))
                {
                    storeType = storeType.Substring(0, openParen);
                }
                Sql.Append(storeType);
            }
        }

        /// <summary>
        /// BigQuery does not support the ESCAPE clause in LIKE expressions.
        /// Override to generate LIKE without ESCAPE.
        /// </summary>
        protected override void GenerateLike(LikeExpression likeExpression, bool negated)
        {
            Visit(likeExpression.Match);

            if (negated)
            {
                Sql.Append(" NOT");
            }

            Sql.Append(" LIKE ");
            Visit(likeExpression.Pattern);
        }

        /// <summary>
        /// BQ does not support the simple CASE syntax when the operand is a boolean expression.
        /// For example: CASE (expr = 'Foo') WHEN TRUE THEN result END
        /// This must be converted to searched CASE: CASE WHEN expr = 'Foo' THEN result END
        /// </summary>
        protected override Expression VisitCase(CaseExpression caseExpression)
        {
            if (caseExpression.Operand != null)
            {
                Sql.Append("CASE");

                using (Sql.Indent())
                {
                    foreach (var whenClause in caseExpression.WhenClauses)
                    {
                        Sql.AppendLine().Append("WHEN ");

                        if (whenClause.Test is SqlConstantExpression { Value: bool boolValue })
                        {
                            if (boolValue)
                            {
                                // WHEN TRUE -> WHEN
                                Visit(caseExpression.Operand);
                            }
                            else
                            {
                                // WHEN FALSE -> WHEN NOT
                                Sql.Append("NOT (");
                                Visit(caseExpression.Operand);
                                Sql.Append(")");
                            }
                        }
                        else
                        {
                            Visit(caseExpression.Operand);
                            Sql.Append(" = ");
                            Visit(whenClause.Test);
                        }

                        Sql.Append(" THEN ");
                        Visit(whenClause.Result);
                    }

                    if (caseExpression.ElseResult != null)
                    {
                        Sql.AppendLine().Append("ELSE ");
                        Visit(caseExpression.ElseResult);
                    }
                }

                Sql.AppendLine().Append("END");

                return caseExpression;
            }

            return base.VisitCase(caseExpression);
        }

        /// <summary>
        /// BigQuery doesn't support (SELECT 1) in ORDER BY clauses.
        /// When the scalar subquery is just selecting a constant, output the constant directly.
        /// </summary>
        protected override Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubqueryExpression)
        {
            var subquery = scalarSubqueryExpression.Subquery;

            if (subquery.Tables.Count == 0
                && subquery.Predicate == null
                && subquery.Orderings.Count == 0
                && subquery.Projection.Count == 1
                && subquery.Projection[0].Expression is SqlConstantExpression constant)
            {
                Visit(constant);
                return scalarSubqueryExpression;
            }

            return base.VisitScalarSubquery(scalarSubqueryExpression);
        }
    }
}