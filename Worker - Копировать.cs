

using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using System.IO.Compression;
using System.Net;
using WorkerUnTarService.Models;
using WorkerUnTarService;
using Amazon.S3.Transfer;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.IO;

namespace AWSUploadService
{
    public class Worker1 : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IAmazonS3 _s3Client;

        private readonly string _sourceFolder;
        private readonly string _archiveDirectory;
        private readonly uint _archiveСlearedSeconds;
        private readonly bool _toArchivSourceFolder;

        private readonly string _minioEndpoint;
        private readonly string _region;
        private readonly string _accessKey;
        private readonly string _secretKey;
        private readonly string _bucketName;

        public Worker1(ILogger<Worker> logger, IHostEnvironment environment, IOptions<AWSUploadSettings> options, IOptions<AWS> awsOptions)
        {

            _logger = logger;
            _logger.LogInformation($"** {typeof(Worker).Name} Logger initialized **"); ;
            _sourceFolder = options.Value.SourceFolder;
            _archiveDirectory = options.Value.ArchiveDirectory;
            _archiveСlearedSeconds = options.Value.ArchiveСlearedSeconds;
            _toArchivSourceFolder = options.Value.ToArchivSourceFolder;

            _minioEndpoint = awsOptions.Value.S3Endpoint;
            _accessKey = awsOptions.Value.AccessKey;
            _secretKey = awsOptions.Value.SecretKey;
            _bucketName = awsOptions.Value.BucketName;
            _region = awsOptions.Value.Region;

            var clientConfig = new MyAmazonS3Config
            {
                AuthenticationRegion = _region,
                ServiceURL = _minioEndpoint,
                ForcePathStyle = true,
                BucketName = _bucketName,
                //Timeout = TimeSpan.FromSeconds(10),
                //RetryMode = RequestRetryMode.Standard,
                //MaxErrorRetry = 3
            };
            _s3Client = new AmazonS3Client(_accessKey, _secretKey, clientConfig);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _logger.LogInformation($"** {typeof(Worker).Name} SERVICE STARTED **"); ;
                while (!stoppingToken.IsCancellationRequested)
                {
                    var directories = Directory.GetDirectories(_sourceFolder);
                    foreach (var subDirectory in directories)
                    {
                        //var old = File.GetLastAccessTime(Directory.GetFiles(subDirectory, "*", SearchOption.AllDirectories).OrderBy(x => File.GetLastAccessTime(x)).Last()); //null

                        if (Directory.GetFiles(subDirectory, "*", SearchOption.AllDirectories).Any())
                        {
                            //  DateTime lastWriteTime = Directory.GetLastWriteTime(subDirectory);

                            DateTime lastWriteTime = File.GetLastAccessTime(Directory.GetFiles(subDirectory, "*", SearchOption.AllDirectories).OrderBy(x => File.GetLastWriteTime(x)).Last());

                            //if ((DateTime.Now - lastWriteTime).TotalSeconds > 60 || (DateTime.Now - lastWriteTime).TotalSeconds < 0)
                            if ((DateTime.Now - lastWriteTime).TotalSeconds < 0)
                                _logger.LogWarning($"** {typeof(Worker).Name} SERVICE: В папке загрузки обнаружены файлы из будущего - {lastWriteTime.ToLongDateString}. **");
                            if ((DateTime.Now - lastWriteTime).TotalSeconds > 30)
                            {
                                var parentDirName = new DirectoryInfo(subDirectory).Name;
                                var ssubDirs = Directory.GetDirectories(subDirectory);
                                GreateNotDeleteFile(Path.Combine(subDirectory, parentDirName + ".notDelete"));
                                
                                    foreach (var dir in ssubDirs)
                                    {
                                        if (!Directory.GetFiles(dir, "*", SearchOption.AllDirectories).Any())
                                            continue;
                                        var result = await FolderToZipAsync(dir);
                                        if (result.Contains(Path.GetFileName(dir)))
                                            switch (_toArchivSourceFolder)
                                            {
                                                case true:
                                                    MoveDir(dir, _archiveDirectory);
                                                    break;
                                                default:
                                                    Directory.Delete(dir, true);
                                                    break;
                                            }
                                    }

                                var myS3Config = (MyAmazonS3Config)_s3Client.Config;
                                var archives = Directory.GetFiles(subDirectory);
                                foreach (var archive in archives)
                                {
                                    if (Path.GetExtension(archive).ToLower() == ".notdelete")
                                        continue;
                                    var request = new PutObjectRequest()
                                    {
                                        
                                        BucketName = _bucketName,
                                        Key = parentDirName + "/" + Path.GetFileName(archive),
                                        FilePath = archive
                                    };
                                    // Starting the Stopwatch 
                                    //var watch = Stopwatch.StartNew();
                                    AmazonWebServiceResponse response;
                                    var fileToUpload = new FileInfo(request.FilePath);
                                    if (fileToUpload.Length < 2000 * (long)Math.Pow(2, 20))
                                         response = await SendToS3Storage(request);
                                    else
                                         response = await TransferToS3StoregeAsync(request);
                                    //watch.Stop();
                                    if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                                    {
                                        
                                        switch (_toArchivSourceFolder)
                                        {
                                            case true:
                                                var dateFolder = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm");
                                                var destinationFolder = Path.Combine(_archiveDirectory, dateFolder);
                                                Directory.CreateDirectory(destinationFolder);
                                                var destinationPath = Path.Combine(destinationFolder, Path.GetFileName(archive));
                                                File.Move(archive, destinationPath);
                                                break;
                                            default:
                                                DeleteFile(archive);
                                                break;
                                        }
                                    }
                                    else
                                        _logger.LogError($"** {typeof(Worker).Name} SERVICE ERROR: не удалось переместить архив {archive} в S3 хранилище. {response.HttpStatusCode} {response.ResponseMetadata}**");
                                }
                            }
                        }
                    }
                    if (_archiveСlearedSeconds != 0)
                        CleanArchive(_archiveСlearedSeconds);
                    await Task.Delay(10000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"** {typeof(Worker).Name} SERVICE ERROR: {ex.Message} **");
                Environment.Exit(1);
            }
            finally
            {
                _logger.LogInformation($"** {typeof(Worker).Name} SERVICE STOPPED **");
            }
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Starting  {typeof(Worker).Name} SERVICE");
            return base.StartAsync(cancellationToken);
        }
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Stopping{typeof(Worker).Name} SERVICE");
            return base.StopAsync(cancellationToken);
        }
        private async Task<string> FolderToZipAsync(string folderPach)
        {
            string zipPach = string.Empty;
            await Task.Run(async () =>
            {
                await Task.Delay(1);
                string zipFileName = folderPach + "_" + DateTime.Now.ToString("dd_MM_yy_hh_mm_ss") + ".zip";
                zipFileName = GetUniquFileName(zipFileName);
                ZipFile.CreateFromDirectory(folderPach, zipFileName, CompressionLevel.Fastest, false);
                zipPach = zipFileName;
            });
            return zipPach;
        }
        private void MoveDir(string sourceDir, string destDir)
        {
            if (Directory.Exists(sourceDir))
            {
                destDir = GetUniquDirName(Path.Combine(destDir, Path.GetFileName(sourceDir)));
                // Проверяем, находятся ли исходная и целевая директории на одном диске
                if (Path.GetPathRoot(sourceDir) == Path.GetPathRoot(destDir))
                {
                    // Если да, то просто перемещаем директорию
                    Directory.Move(sourceDir, destDir);
                }
                else
                {
                    // Если нет, то копируем директорию и ее содержимое
                    CopyDirectory(sourceDir, destDir);
                    // Затем удаляем исходную директорию
                    Directory.Delete(sourceDir, true);
                }
            }
            else
            {
                // Если исходная директория не существует, выводим сообщение об ошибке
                _logger.LogError($"** {typeof(Worker).Name}: Исходная директория для перемещения {sourceDir} не сущестует **");
            }
        }
        public static void CopyDirectory(string sourceDir, string destDir)
        {
            // Создаем целевую директорию, если она не существует
            if (!Directory.Exists(destDir))
            {
                Directory.CreateDirectory(destDir);
            }
            // Получаем список файлов в исходной директории
            string[] files = Directory.GetFiles(sourceDir);
            // Копируем каждый файл в целевую директорию
            foreach (string file in files)
            {
                string fileName = Path.GetFileName(file);
                string destFile = Path.Combine(destDir, fileName);
                File.Copy(file, destFile, true);
            }
            // Получаем список поддиректорий в исходной директории
            string[] dirs = Directory.GetDirectories(sourceDir);
            // Рекурсивно копируем каждую поддиректорию в целевую директорию
            foreach (string dir in dirs)
            {
                string dirName = Path.GetFileName(dir);
                string destDirName = Path.Combine(destDir, dirName);
                CopyDirectory(dir, destDirName);
            }
        }
        private static string GetUniquFileName(string filename, string basename = "", int index = 0)
        {
            if (basename == "")
                basename = Path.GetFileName(filename);
            string folderPath = Path.GetDirectoryName(filename);
            string uniqumFileName = filename;
            if (File.Exists(uniqumFileName))
            {
                index++;
                var splitedName = Path.GetFileName(basename).Split(".");
                filename = Path.Combine(folderPath, splitedName[splitedName.Length - 2] + $" ({index}).{splitedName.Last()}");
                uniqumFileName = GetUniquFileName(filename, basename, index);
            }
            return uniqumFileName;
        }
        private static string GetUniquDirName(string filename, string basename = "", int index = 0)
        {
            if (basename == "")
                basename = Path.GetFileName(filename);
            string folderPath = Path.GetDirectoryName(filename);
            string uniqumFileName = filename;
            if (Directory.Exists(uniqumFileName))
            {
                index++;
                //var splitedName = Path.GetFileName(basename).Split(".");
                filename = Path.Combine(folderPath, $"{basename}({index})");
                uniqumFileName = GetUniquDirName(filename, basename, index);
            }
            return uniqumFileName;
        }
        private async Task<PutObjectResponse> SendToS3Storage(PutObjectRequest request)
        {
            //var initResponse = _s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            //{
            //    BucketName = request.BucketName,
            //    Key = request.Key,
            //});
            //var uploadPartRequest = new UploadPartRequest
            //{
            //    BucketName = request.BucketName,
            //    FilePath = request.FilePath,
            //    Key = request.Key,
            //    PartNumber = 1,
            //    UploadId = initResponse.Result.UploadId,
            //};
            //uploadPartRequest.StreamTransferProgress += OnTransferProgress;
            //var uploadPartResponse = await _s3Client.UploadPartAsync(uploadPartRequest);

            //return new PutObjectResponse();

            PutObjectResponse response;
            try
            {
                response = await _s3Client.PutObjectAsync(request).ConfigureAwait(true);
                return response;
            }
            catch (AmazonS3Exception ex)
            {
                _logger.LogError($"** {typeof(Worker).Name} SERVICE ERROR: {ex.Message} **");
                return new PutObjectResponse
                { HttpStatusCode = HttpStatusCode.ExpectationFailed };
            }
            catch (Exception ex)
            {
                _logger.LogError($"** {typeof(Worker).Name} SERVICE ERROR: {ex.Message} **");
                return new PutObjectResponse
                { HttpStatusCode = HttpStatusCode.ExpectationFailed };
            }

        }
        private async Task<CompleteMultipartUploadResponse> TransferToS3StoregeAsync(PutObjectRequest request)
        {
            // Define the file transfer utility and the file to upload.
            //var fileTransferUtility = new TransferUtility(_s3Client);
            var fileToUpload = new FileInfo(request.FilePath);

            // Step 1: Initialize the multipart upload.
            var initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = request.BucketName,
                Key = request.Key
            };
            var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);

            // Step 2: Upload the file parts.
            var partETags = new List<PartETag>();
            long partSize = 1000 * (long)Math.Pow(2, 20); // 500 MB
            long filePosition = 0;
            for (int i = 1; filePosition < fileToUpload.Length; i++)
            {
                partSize = Math.Min(partSize, (fileToUpload.Length - filePosition));
                using (var fileStream = new FileStream(request.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920))
                {
                   
                    fileStream.Position = filePosition;
                    var uploadRequest = new UploadPartRequest
                    {
                        BucketName = request.BucketName,
                        Key = request.Key,
                        UploadId = initiateResponse.UploadId,
                        PartNumber = i,
                        PartSize = partSize,
                        FilePosition = filePosition,
                        InputStream = fileStream
                    };

                    // Upload part and add the returned ETag to our list.
                    var uploadResponse = await _s3Client.UploadPartAsync(uploadRequest);
                    partETags.Add(new PartETag
                    {
                        PartNumber = i,
                        ETag = uploadResponse.ETag
                    });
                }
                filePosition += partSize;
            }

            // Step 3: Complete the multipart upload.
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = request.BucketName,
                Key = request.Key,
                UploadId = initiateResponse.UploadId,
                PartETags = partETags
            };
            return await _s3Client.CompleteMultipartUploadAsync(completeRequest);
        }
        private void OnTransferProgress(object? sender, StreamTransferProgressArgs e)
        {
            Console.WriteLine("{0}/{1} {2}%", e.TransferredBytes, e.TotalBytes, e.PercentDone);
        }
        private void CleanArchive(uint time)
        {
            //Удаление архивированных каталогов
            foreach (var directory in Directory.GetDirectories(_archiveDirectory))
            {
                if ((DateTime.Now - Directory.GetCreationTime(directory)).TotalSeconds > time)
                {
                    Directory.Delete(directory, true);
                }
            }
        }
        private void GreateNotDeleteFile(string path)
        {
            if (!File.Exists(path))
            {
                using (File.Create(path)) ;
            }
        }
        private void DeleteFile(string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    FileAttributes attributes = File.GetAttributes(path);
                    if ((attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                    {
                        attributes &= ~FileAttributes.ReadOnly;
                        File.SetAttributes(path, attributes);
                        _logger.LogWarning($"** {typeof(Worker).Name} SERVICE: Aтрибут только для чтения снят с файла {path} из папки загрузки. **");
                    }
                        File.Delete(path);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"** {typeof(Worker).Name} SERVICE ERROR: не удалось удалить архив {path} из папки загрузки. {ex.Message}**");
            }
        }




        private async Task<CompleteMultipartUploadResponse> UploadFileAsync2(PutObjectRequest request)
        {

            var config = new TransferUtilityConfig
            {
                // Максимальное количество одновременных потоков для загрузки.
                // Увеличение этого числа может улучшить пропускную способность при высокоскоростном интернет-соединении.
                //ConcurrentServiceRequests = 50,
                //MinSizeBeforePartUpload = 500,

            };
            var test = 500 * (long)Math.Pow(2, 20);
            var transferUtility = new TransferUtility(_s3Client, config);
            try
            {
                // Путь к файлу, который вы хотите загрузить
                string filePath = request.FilePath;

                // Настройка запроса на многокомпонентную загрузку
                var uploadRequest = new TransferUtilityUploadRequest
                {

                    BucketName = request.BucketName,
                    FilePath = filePath,
                    StorageClass = S3StorageClass.Standard,
                    PartSize = 5000 * (long)Math.Pow(2, 20), // Размер части в байтах (например, 1000 МБ)
                    Key = request.Key
                };

                // Загрузка файла
                await transferUtility.UploadAsync(uploadRequest);
                return new CompleteMultipartUploadResponse { HttpStatusCode = HttpStatusCode.OK };
                Console.WriteLine("Файл успешно загружен.");
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Ошибка при загрузке: " + e.Message);
                return new CompleteMultipartUploadResponse { HttpStatusCode = HttpStatusCode.PreconditionFailed };
            }
        }
        private async Task<CompleteMultipartUploadResponse> SendToS3Storage2(PutObjectRequest request)
        {
            // Определение утилиты передачи файлов и файла для загрузки.
            var fileTransferUtility = new TransferUtility(_s3Client);
            var fileToUpload = new FileInfo(request.FilePath);

            // Шаг 1: Инициализация многокомпонентной загрузки.
            var initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = request.BucketName,
                Key = request.Key
            };
            var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);

            // Шаг 2: Загрузка частей файла.
            var partETags = new ConcurrentBag<PartETag>();
            long partSize = 1000 * (long)Math.Pow(2, 20); // 1000 MB
            long filePosition = 0;
            var tasks = new List<Task>();

            while (filePosition < fileToUpload.Length)
            {
                partSize = Math.Min(partSize, (fileToUpload.Length - filePosition));
                var partNumber = tasks.Count + 1;
                tasks.Add(UploadPartAsync(_s3Client, initiateResponse.UploadId, request.BucketName, request.Key, request.FilePath, partNumber, partSize, filePosition, partETags));
                filePosition += partSize;
            }

            await Task.WhenAll(tasks);

            // Шаг 3: Завершение многокомпонентной загрузки.
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = request.BucketName,
                Key = request.Key,
                UploadId = initiateResponse.UploadId,
                PartETags = partETags.ToList()
            };
            return await _s3Client.CompleteMultipartUploadAsync(completeRequest);

            // Асинхронный метод для загрузки части файла.
            async Task UploadPartAsync(IAmazonS3 s3Client, string uploadId, string bucketName, string key, string filePath, int partNumber, long partSize, long filePosition, ConcurrentBag<PartETag> partETags)
            {
                using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 16384))
                {
                    fileStream.Position = filePosition;
                    var uploadRequest = new UploadPartRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        UploadId = uploadId,
                        PartNumber = partNumber,
                        PartSize = partSize,
                        FilePosition = filePosition,
                        InputStream = fileStream
                    };

                    var uploadResponse = await s3Client.UploadPartAsync(uploadRequest);
                    partETags.Add(new PartETag
                    {
                        PartNumber = partNumber,
                        ETag = uploadResponse.ETag
                    });
                }
            }

        }
    }
}

