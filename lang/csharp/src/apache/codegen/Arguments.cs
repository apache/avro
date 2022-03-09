using System.Collections.Generic;

namespace Avro
{
    internal class Arguments
    {
        public Arguments()
        {
            NamespaceMapping = new Dictionary<string, string>();
            CreateNamespaceDirectories = true;
        }

        public bool CreateNamespaceDirectories { get; set; }
        public bool HasInput => !string.IsNullOrEmpty(Input);
        public bool HasNamespaceMapping => NamespaceMapping.Count > 0;
        public bool HasOutputDirectory => !string.IsNullOrEmpty(OutputDirectory);
        public string Input { get; set; }
        public Dictionary<string, string> NamespaceMapping { get; set; }
        public string OutputDirectory { get; set; }
    }
}
