using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Helper.DBHelper
{
    [System.AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public sealed class EntityFieldMapper : System.Attribute
    {
        public string Field { get; private set; }

        public EntityFieldMapper(string field)
        {
            if (String.IsNullOrWhiteSpace(field))
                throw new ArgumentException("Entity field mapper cannot be null or empty");

            this.Field = field;
        }
    }
}
