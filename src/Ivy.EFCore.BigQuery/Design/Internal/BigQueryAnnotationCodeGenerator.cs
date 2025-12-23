using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Ivy.EntityFrameworkCore.BigQuery.Design.Internal
{
    internal class BigQueryAnnotationCodeGenerator : AnnotationCodeGenerator
    {
        public BigQueryAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
            : base(dependencies)
        {

        }

        protected override bool IsHandledByConvention(IModel model, IAnnotation annotation)
        {
            if (annotation.Name.StartsWith("BigQuery:"))
            {
                return true;
            }

            return base.IsHandledByConvention(model, annotation);
        }

        protected override bool IsHandledByConvention(IProperty property, IAnnotation annotation)
        {
            if (annotation.Name.StartsWith("BigQuery:"))
            {
                return true;
            }

            return base.IsHandledByConvention(property, annotation);
        }
    }
}