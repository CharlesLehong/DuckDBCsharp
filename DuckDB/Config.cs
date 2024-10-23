using System.Text.Json.Serialization;

namespace DuckDB
{
    public class Config
    {
        [JsonPropertyName("ratingProcessId")]
        public int? RatingProcessId { get; set; }

        [JsonPropertyName("ratingProcessVersionId")]
        public int? RatingProcessVersionId { get; set; }

        [JsonPropertyName("processTaskFileStoreIds")]
        public int[] ProcessTaskFileStoreIds { get; set; } = new int[0];

        [JsonPropertyName("processFileTypeConfigs")]
        public ProcessFileTypeConfig[] ProcessFileTypeConfigs { get; set; } = new ProcessFileTypeConfig[0];

        [JsonPropertyName("outputProcessTaskFileStoreId")]
        public int? OutputProcessTaskFileStoreId { get; set; }
    }
}
