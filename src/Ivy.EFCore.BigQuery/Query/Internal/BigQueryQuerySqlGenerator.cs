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
                //BigQueryUnnestExpression unnestExpression => VisitBigQueryUnnest(unnestExpression),
                //BigQueryArrayAccessExpression arrayAccessExpression => VisitBigQueryArrayAccess(arrayAccessExpression),
                BigQueryStructAccessExpression structAccessExpression => VisitBigQueryStructAccess(structAccessExpression),
                //BigQueryArrayConstructorExpression arrayConstructorExpression => VisitBigQueryArrayConstructor(arrayConstructorExpression),
                BigQueryStructConstructorExpression structConstructorExpression => VisitBigQueryStructConstructor(structConstructorExpression),
                _ => base.VisitExtension(extensionExpression)
            };
        }

            //protected virtual Expression VisitBigQueryUnnest(BigQueryUnnestExpression unnestExpression)
            //{
            //    Sql.Append("UNNEST(");
            //    Visit(unnestExpression.ArrayExpression);
            //    Sql.Append(")").Append(AliasSeparator).Append(_sqlGenerationHelper.DelimitIdentifier(unnestExpression.Alias));

            //    if (unnestExpression.WithOffset)
            //    {
            //        Sql.Append(unnestExpression.UseOrdinal ? " WITH ORDINAL" : " WITH OFFSET");
            //        if (!string.IsNullOrEmpty(unnestExpression.OffsetAlias))
            //        {
            //            Sql.Append(AliasSeparator).Append(_sqlGenerationHelper.DelimitIdentifier(unnestExpression.OffsetAlias));
            //        }
            //    }

            //    return unnestExpression;
            //}

            //protected virtual Expression VisitBigQueryArrayAccess(BigQueryArrayAccessExpression arrayAccessExpression)
            //{
            //    Visit(arrayAccessExpression.Array);
            //    Sql.Append("[");
            //    Sql.Append(arrayAccessExpression.UseOrdinal ? "ORDINAL(" : "OFFSET(");
            //    Visit(arrayAccessExpression.Index);
            //    Sql.Append(")]");

            //    return arrayAccessExpression;
            //}

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