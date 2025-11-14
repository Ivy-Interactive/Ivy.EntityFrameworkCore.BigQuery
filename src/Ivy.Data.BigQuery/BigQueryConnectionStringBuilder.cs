using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.Data.BigQuery
{
    public class BigQueryConnectionStringBuilder : DbConnectionStringBuilder
    {
        private const string ProjectIdKey = "ProjectId";
        private const string DefaultDatasetIdKey = "DefaultDatasetId";
        private const string LocationKey = "Location";
        private const string AuthMethodKey = "AuthMethod";
        private const string CredentialsFileKey = "CredentialsFile";
        private const string TimeoutKey = "Timeout"; 
        private const BigQueryAuthMethod DefaultAuthMethod = BigQueryAuthMethod.ApplicationDefaultCredentials; 
        private const int DefaultTimeout = 15;

        public BigQueryConnectionStringBuilder()
        {
        }

        public BigQueryConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public enum BigQueryAuthMethod
        {
            ApplicationDefaultCredentials,
            JsonCredentials,
        }

        [DisplayName(ProjectIdKey)]
        [Description("The Google Cloud Project ID to connect to (Required).")]
        public string ProjectId
        {
            get => GetValueOrDefault<string>(ProjectIdKey, null);
            set => base[ProjectIdKey] = value;
        }

        [DisplayName(DefaultDatasetIdKey)]
        [Description("The default Google BigQuery Dataset ID for unqualified table names (Optional).")]
        public string DefaultDatasetId
        {
            get => GetValueOrDefault<string>(DefaultDatasetIdKey, null);
            set => SetOptionalStringValue(DefaultDatasetIdKey, value);
        }

        [DisplayName(LocationKey)]
        [Description("The geographic location hint where jobs should be run (e.g., 'US', 'EU', 'asia-northeast1') (Optional).")]
        public string Location
        {
            get => GetValueOrDefault<string>(LocationKey, null);
            set => SetOptionalStringValue(LocationKey, value);
        }

        [DisplayName(AuthMethodKey)]
        [Description("The authentication method ('ApplicationDefaultCredentials' or 'JsonCredentials'). Defaults to 'ApplicationDefaultCredentials' if not specified.")]
        public BigQueryAuthMethod AuthMethod
        {
            get
            {
                if (!base.TryGetValue(AuthMethodKey, out object value) || value is not string strValue)
                    return DefaultAuthMethod;
                if (Enum.TryParse<BigQueryAuthMethod>(strValue, true, out var result))
                {
                    return result;
                }

                throw new ArgumentException($"Invalid value for {AuthMethodKey}: {strValue}");
            }
            set => base[AuthMethodKey] = value.ToString();
        }

        [DisplayName(CredentialsFileKey)]
        [Description("Full path to the JSON service account key file (Required if AuthMethod is JsonCredentials).")]
        public string CredentialsFile
        {
            get => GetValueOrDefault<string>(CredentialsFileKey, null);
            set => SetOptionalStringValue(CredentialsFileKey, value);
        }

        [DisplayName(TimeoutKey)]
        [Description("Connection timeout in seconds (for authentication)")]
        public int Timeout
        {
            get
            {
                if (!base.TryGetValue(TimeoutKey, out object value)) return DefaultTimeout;

                return value switch
                {
                    int intValue => intValue,
                    string strValue when int.TryParse(strValue, NumberStyles.Integer, CultureInfo.InvariantCulture,
                        out int parsedValue) => parsedValue,
                    _ => DefaultTimeout
                };
            }
            set
            {
                ArgumentOutOfRangeException.ThrowIfNegative(value);
                base[TimeoutKey] = value.ToString(CultureInfo.InvariantCulture);
            }
        }

        public override bool Remove(string keyword)
        {
            var removed = base.Remove(keyword);
            return removed;
        }

        private T GetValueOrDefault<T>(string key, T defaultValue)
        {
            if (!base.TryGetValue(key, out object value)) return defaultValue;

            try
            {
                return value == null || value == DBNull.Value ? defaultValue : (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is InvalidCastException || ex is FormatException || ex is OverflowException)
            {
                return defaultValue;
            }
        }

        private void SetOptionalStringValue(string key, string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                base[key] = value;
            }
            else
            {
                base.Remove(key);
            }
        }
    }
}
