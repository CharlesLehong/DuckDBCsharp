using Azure.Storage.Blobs;
using Bootsure.Processing.Common.Models;
using Bootsure.Processing.Common.Repositories;
using Bootsure.Processing.Common.Services;
using DuckDB;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddUserSecrets<Program>()
                .Build();

var blobServiceClient = new BlobServiceClient(configuration["BlobConnectionString"]);
var repository = new Repository(configuration.GetConnectionString("BootsureDB"));
var processingData = new ProcessingData()
{
    OrganisationId = "c8b3700f-d3b3-4dfa-b131-f9eee6afb43e",
    ProcessTaskId = 5,
    TaskType = ""
};

var dbname = Guid.NewGuid().ToString(); 
var processingManager = new ProcessingManager(repository, blobServiceClient, processingData);
var duckDbRepository = new DuckDBRepository(dbname);

var duckDBService = new DuckDBService(repository, duckDbRepository, processingManager, blobServiceClient, processingData.OrganisationId);
await duckDBService.ProcessTaskAsync();

