
using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.Linq;
using Xunit;

namespace Ivy.EntityFrameworkCore.BigQuery.Query
{
    public class StringFunctionsQueryBigQueryTest : IClassFixture<NorthwindQueryBigQueryFixture<BigQueryNorthwindModelCustomizer>>
    {
        private readonly NorthwindQueryBigQueryFixture<BigQueryNorthwindModelCustomizer> _fixture;

        public StringFunctionsQueryBigQueryTest(NorthwindQueryBigQueryFixture<BigQueryNorthwindModelCustomizer> fixture)
        {
            _fixture = fixture;
            _fixture.TestSqlLoggerFactory.Clear();
        }

        [Fact]
        public void String_ToLower_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.CustomerID.ToLower() == "alfki").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE LOWER(`c`.`CustomerID`) = 'alfki'
""");
        }

        [Fact]
        public void String_ToUpper_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.CustomerID.ToUpper() == "ALFKI").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE UPPER(`c`.`CustomerID`) = 'ALFKI'
""");
        }

        [Fact]
        public void String_StartsWith_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.StartsWith("Maria")).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE `c`.`ContactName` LIKE 'Maria%'
""");
        }

        [Fact]
        public void String_EndsWith_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.EndsWith("Anders")).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE `c`.`ContactName` LIKE '%Anders'
""");
        }

        [Fact]
        public void String_Contains_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.Contains("and")).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE `c`.`ContactName` LIKE '%and%'
""");
        }

        [Fact]
        public void String_Replace_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.Replace("a", "o") == "Morio Anders").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE REPLACE(`c`.`ContactName`, 'a', 'o') = 'Morio Anders'
""");
        }

        [Fact]
        public void String_Substring_with_one_arg_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.Substring(3) == "ia Anders").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE SUBSTR(`c`.`ContactName`, 4) = 'ia Anders'
""");
        }

        [Fact]
        public void String_Substring_with_two_args_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.Substring(0, 5) == "Maria").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE SUBSTR(`c`.`ContactName`, 1, 5) = 'Maria'
""");
        }

        [Fact]
        public void String_IndexOf_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.IndexOf("a") == 1).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE (STRPOS(`c`.`ContactName`, 'a') - 1) = 1
""");
        }

        [Fact]
        public void String_Trim_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.Trim() == "Maria Anders").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE TRIM(`c`.`ContactName`) = 'Maria Anders'
""");
        }

        [Fact]
        public void String_TrimStart_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.TrimStart() == "Maria Anders").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE LTRIM(`c`.`ContactName`) = 'Maria Anders'
""");
        }

        [Fact]
        public void String_TrimEnd_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.TrimEnd() == "Maria Anders").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE RTRIM(`c`.`ContactName`) = 'Maria Anders'
""");
        }

        [Fact]
        public void String_Length_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.ContactName.Length > 10).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE LENGTH(`c`.`ContactName`) > 10
""");
        }

        [Fact]
        public void String_IsNullOrEmpty_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => string.IsNullOrEmpty(c.Region)).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE (`c`.`Region` IS NULL) OR (`c`.`Region` = '')
""");
        }

        [Fact]
        public void String_IsNullOrWhiteSpace_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => string.IsNullOrWhiteSpace(c.Region)).ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE (`c`.`Region` IS NULL) OR (TRIM(`c`.`Region`) = '')
""");
        }

        [Fact]
        public void String_PadLeft_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.CustomerID.PadLeft(10) == "     ALFKI").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE LPAD(`c`.`CustomerID`, 10) = '     ALFKI'
""");
        }

        [Fact]
        public void String_PadLeft_with_char_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.CustomerID.PadLeft(10, '0') == "0000ALFKI").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE LPAD(`c`.`CustomerID`, 10, '0') = '0000ALFKI'
""");
        }

        [Fact]
        public void String_PadRight_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.CustomerID.PadRight(10) == "ALFKI     ").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE RPAD(`c`.`CustomerID`, 10) = 'ALFKI     '
""");
        }

        [Fact]
        public void String_PadRight_with_char_is_translated()
        {
            using var context = _fixture.CreateContext();
            var _ = context.Customers.Where(c => c.CustomerID.PadRight(10, '0') == "ALFKI00000").ToList();

            AssertSql(
                """
SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region`
FROM `Customers` AS `c`
WHERE RPAD(`c`.`CustomerID`, 10, '0') = 'ALFKI00000'
""");
        }

        // Note: String.Join is not yet supported in Phase 2 due to complexity with expression translation
        // It requires custom SQL expression types or preprocessing at an earlier stage in the query pipeline

        private void AssertSql(string expected)
        {
            _fixture.TestSqlLoggerFactory.AssertBaseline(new[] { expected });
        }
    }
}
