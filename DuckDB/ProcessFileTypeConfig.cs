using System.Text.Json.Serialization;

namespace DuckDB
{
    public class ProcessFileTypeConfig
    {
        [JsonPropertyName("processFileTypeId")]
        public int ProcessFileTypeId { get; set; }

        [JsonPropertyName("processFileType")]
        public string ProcessFileType { get; set; } = string.Empty;

        [JsonPropertyName("dataModelFieldId")]
        public int DataModelFieldId { get; set; }

        [JsonPropertyName("dataModelField")]
        public string DataModelField { get; set; } = string.Empty;

        [JsonPropertyName("dataModelVersionId")]
        public int? DataModelVersionId { get; set; }

        [JsonPropertyName("allowsMutipleUploads")]
        public bool AllowsMutipleUploads { get; set; }
    }
}
