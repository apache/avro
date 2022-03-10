using System.Collections.Generic;

namespace Avro
{
    public class Arguments
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Arguments"/> class.
        /// </summary>
        public Arguments()
        {
            NamespaceMapping = new Dictionary<string, string>();
            CreateNamespaceDirectories = true;
        }

        /// <summary>
        /// Gets or sets a value indicating whether [create namespace directories].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [create namespace directories]; otherwise, <c>false</c>.
        /// </value>
        public bool CreateNamespaceDirectories { get; set; }

        /// <summary>
        /// Gets or sets the path to the Protocol or Schema File
        /// </summary>
        /// <value>
        /// The input.
        /// </value>
        public string Input { get; set; }

        /// <summary>
        /// Gets or sets the namespace mapping.
        /// </summary>
        /// <value>
        /// The namespace mapping.
        /// </value>
        public Dictionary<string, string> NamespaceMapping { get; set; }

        /// <summary>
        /// Gets or sets the output directory.
        /// </summary>
        /// <value>
        /// The output directory.
        /// </value>
        public string OutputDirectory { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance has input.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance has input; otherwise, <c>false</c>.
        /// </value>
        internal bool HasInput => !string.IsNullOrEmpty(Input);

        /// <summary>
        /// Gets a value indicating whether this instance has namespace mapping.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance has namespace mapping; otherwise, <c>false</c>.
        /// </value>
        internal bool HasNamespaceMapping => NamespaceMapping.Count > 0;

        /// <summary>
        /// Gets a value indicating whether this instance has output directory.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance has output directory; otherwise, <c>false</c>.
        /// </value>
        internal bool HasOutputDirectory => !string.IsNullOrEmpty(OutputDirectory);
    }
}
