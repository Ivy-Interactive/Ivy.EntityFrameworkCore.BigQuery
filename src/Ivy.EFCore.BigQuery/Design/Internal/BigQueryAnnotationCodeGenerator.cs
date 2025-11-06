using Microsoft.EntityFrameworkCore.Design;

namespace Ivy.EntityFrameworkCore.BigQuery.Design.Internal
{
    internal class BigQueryAnnotationCodeGenerator : AnnotationCodeGenerator
    {
        public BigQueryAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
            : base(dependencies)
        {
            
        }
    }
}