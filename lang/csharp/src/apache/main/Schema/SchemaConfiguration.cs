namespace Avro
{
    /// <summary>
    /// Schema configuration
    /// </summary>
    public static class SchemaConfiguration
    {
        /// <summary>
        /// Enables 'soft-match' algorithm for compatibility java and .net consumers
        /// see https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/Resolver.java#L640
        /// </summary>
        public static bool UseSoftMatch { get; set; }
    }
}
