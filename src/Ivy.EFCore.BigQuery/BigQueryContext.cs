using Google.Cloud.BigQuery.V2;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace Ivy.EntityFrameworkCore.BigQuery
{
    public class BigQueryContext : DbContext
    {
        private readonly string _projectId;
        private readonly string _datasetId;
        private readonly BigQueryClient _client;

        public BigQueryContext(string projectId, string datasetId)
        {
            _projectId = projectId;
            _datasetId = datasetId;
            _client = BigQueryClient.Create(projectId);
        }

        public IQueryable<T> Query<T>(string tableName) where T : class
        {
            var query = $"SELECT * FROM `{_projectId}.{_datasetId}.{tableName}` LIMIT 10";
            var result = _client.ExecuteQuery(query, parameters: null);
            

            return result.Select(row => MapRowToEntity<T>(row)).AsQueryable();
        }

        private T MapRowToEntity<T>(BigQueryRow row) where T : class
        {
            var entity = Activator.CreateInstance<T>();
            var properties = typeof(T).GetProperties();

            foreach (var property in properties)
            {
                var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name ?? property.Name;

                if (!row.Schema.Fields.Any(f => f.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase)))
                    continue;
                
                var value = row[columnName];
                if (value == null) 
                    continue;
                    
                if (property.PropertyType == typeof(int) || property.PropertyType == typeof(int?))
                {
                    property.SetValue(entity, Convert.ToInt32(value));
                }
                else if (property.PropertyType == typeof(long) || property.PropertyType == typeof(long?))
                {
                    property.SetValue(entity, value);
                }
                else if (property.PropertyType == typeof(float) || property.PropertyType == typeof(float?))
                {
                    property.SetValue(entity, (float)Convert.ToDouble(value));
                }
                else if (property.PropertyType == typeof(double) || property.PropertyType == typeof(double?))
                {
                    property.SetValue(entity, value);
                }
                else if (property.PropertyType == typeof(string))
                {
                    property.SetValue(entity, value.ToString());
                }
                else if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(DateTime?))
                {
                    property.SetValue(entity, Convert.ToDateTime(value));
                }
                else
                {
                    property.SetValue(entity, Convert.ChangeType(value, property.PropertyType));
                }
               
            }

            return entity;
        }
    }
}
