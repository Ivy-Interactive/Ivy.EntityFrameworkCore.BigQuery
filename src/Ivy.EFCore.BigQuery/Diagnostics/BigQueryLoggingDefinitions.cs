using Microsoft.EntityFrameworkCore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Diagnostics
{
    public class BigQueryLoggingDefinitions : RelationalLoggingDefinitions
    {
        public EventDefinitionBase? LogSchemaConfigured;

        public EventDefinitionBase? LogSequenceConfigured;

        public EventDefinitionBase? LogUsingSchemaSelectionsWarning;

        public EventDefinitionBase? LogFoundColumn;

        public EventDefinitionBase? LogFoundForeignKey;

        public EventDefinitionBase? LogForeignKeyScaffoldErrorPrincipalTableNotFound;

        public EventDefinitionBase? LogFoundTable;

        public EventDefinitionBase? LogMissingTable;

        public EventDefinitionBase? LogPrincipalColumnNotFound;

        public EventDefinitionBase? LogFoundIndex;

        public EventDefinitionBase? LogFoundPrimaryKey;

        public EventDefinitionBase? LogFoundUniqueConstraint;

        public EventDefinitionBase? LogUnexpectedConnectionType;

        public EventDefinitionBase? LogTableRebuildPendingWarning;

        public EventDefinitionBase? LogCompositeKeyWithValueGeneration;

        public EventDefinitionBase? LogInferringTypes;

        public EventDefinitionBase? LogOutOfRangeWarning;

        public EventDefinitionBase? LogFormatWarning;
    }
}
