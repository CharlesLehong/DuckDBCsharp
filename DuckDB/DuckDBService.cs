using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Bootsure.Processing.Common.Models;
using Bootsure.Processing.Common.Repositories;
using Bootsure.Processing.Common.Services;
using CsvHelper;

namespace DuckDB
{
    public class DuckDBService
    {
        private readonly string _organisationId;
        private readonly Repository _repository;
        private readonly DuckDBRepository _duckDBRepository;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ProcessingManager _processingManager;

        public DuckDBService(Repository repository, DuckDBRepository duckDBRepository, ProcessingManager processingManager,
            BlobServiceClient blobServiceClient, string organisaitonId)
        {
            _repository = repository;
            _duckDBRepository = duckDBRepository;
            _organisationId = organisaitonId;
            _blobServiceClient = blobServiceClient;
            _processingManager = processingManager;
        }

        public async Task ProcessTaskAsync()
        {
            var processTask = _processingManager.ProcessTask;
            await LoadFileStoreAsync(3);

        }

        private async Task LoadFileStoreAsync(int storeId)
        {
            var store = await _repository.GetProcessTaskFileStoreAsync(storeId);
            if (store == null) throw new Exception($"Store with id {storeId} not found in organisation {_organisationId}.");

            var storeModel = await _repository.GetDataModelVersionAsync(store.DataModelId.Value, store.DataModelVersionId.Value);
            if (storeModel == null) throw new Exception($"Store {store.Name} does not have a data model defined.");

            var tableName = store.Name;
            var tableScript = GenerateTableCreationScript(tableName, storeModel.Fields);
            var loadingScrpt = GenerateDataLoadScript(tableName, storeModel.Fields, store.Files);
            var script = string.Concat(tableScript, loadingScrpt);


            await _duckDBRepository.LoadDataAsync(script);
            await WriteQueryResultsToCsv($"SELECT * FROM {tableName}");
            //var results = _duckDBRepository.ExecuteQuery($"SELECT * FROM {tableName}");

            //await foreach (var result in results ) 
            //    Console.WriteLine(result);
        }

        public async Task WriteQueryResultsToCsv(string query)
        {
            using var writer = new StreamWriter("C:\\Users\\lehon\\Downloads\\NPL_2021-02-01\\Charles.csv");
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            bool headersWritten = false;
            await foreach (dynamic row in _duckDBRepository.ExecuteQuery(query))
            {
                if (!headersWritten)
                {
                    foreach (var key in row.Keys)
                        csv.WriteField(key);

                    csv.NextRecord();
                    headersWritten = true;
                }

                foreach (var value in row.Values)
                    csv.WriteField(value);

                csv.NextRecord();
            }
        }

        public async Task WriteRecordsToCsvAsync(string query)
        {
            using (var writer = new StreamWriter("C:\\Users\\lehon\\Downloads\\NPL_2021-02-01\\Charles.csv"))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                await foreach (dynamic record in _duckDBRepository.ExecuteQuery(query))
                {
                    var expandoDict = (IDictionary<string, object>)record;
                    await csv.WriteRecordsAsync(new List<IDictionary<string, object>> { expandoDict });
                }
            }
        }

        private string GenerateTableCreationScript(string tableName, IEnumerable<DataModelField> fields)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"DROP TABLE IF EXISTS {tableName};");
            sb.AppendLine($"CREATE TABLE {tableName} (");

            foreach (var field in fields)
                sb.AppendLine($"\"{field.Name}\" {GetSqlDataType(field.DataTypeId)},");

            sb.Remove(sb.Length - 3, 3);
            sb.AppendLine(");");
            return sb.ToString();
        }

        private string GenerateDataLoadScript(string tableName, IEnumerable<DataModelField> fields,
            IEnumerable<ProcessTaskFile> files)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"INSERT INTO {tableName}");
            sb.AppendLine($"SELECT");

            foreach (var field in fields)
                sb.AppendLine($"\"{field.Name}\",");

            var blobUrls = new List<string>();
            foreach (var file in files)
                blobUrls.Add(GenerateBlobSasUri(file.BlobId.ToString(),
                    _organisationId).ToString());

            sb.Remove(sb.Length - 3, 3);
            sb.AppendLine($" FROM read_parquet(['{string.Join("', '", blobUrls)}'])");
            return sb.ToString();
        }

        private Uri GenerateBlobSasUri(string blobId, string containerName)
        {
            var blobName = $"processtaskfiles/{blobId}";
            BlobContainerClient containerClient = GetOrCreateContainerClient(containerName);
            BlobClient blobClient = containerClient.GetBlobClient(blobName);

            if (!blobClient.CanGenerateSasUri)
            {
                throw new InvalidOperationException("Cannot generate SAS URI.");
            }

            BlobSasBuilder sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = blobClient.GetParentBlobContainerClient().Name,
                BlobName = blobClient.Name,
                Resource = "b",
                StartsOn = DateTimeOffset.UtcNow,
                ExpiresOn = DateTimeOffset.UtcNow.AddHours(1)
            };

            BlobContainerSasPermissions blobContainerSasPermission = BlobContainerSasPermissions.Read;
            sasBuilder.SetPermissions(blobContainerSasPermission);
            return blobClient.GenerateSasUri(sasBuilder);
        }

        private BlobContainerClient GetOrCreateContainerClient(string containerName)
        {
            return _blobServiceClient.GetBlobContainers()
                .FirstOrDefault(c => c.Name == containerName) == null
                ? _blobServiceClient.CreateBlobContainer(containerName)
                : _blobServiceClient.GetBlobContainerClient(containerName);
        }

        private string GetSqlDataType(int dataTypeId)
        {
            return dataTypeId switch
            {
                1 => "VARCHAR",
                2 => "INT",
                3 => "FLOAT",
                4 => "DATE",
                8 => "BIT",
                _ => "TEXT"
            };
        }
    }
}
