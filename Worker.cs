using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Threading.Channels; // Важное дополнение
using WorkerUnTarService;
using WorkerUnTarService.Models;

namespace AWSUploadService
{
    // Модели для передачи данных между каналами
    public record FolderTask(string FolderPath, string ParentDirName);
    public record UploadTask(string FilePath, string ParentDirName);

    
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IAmazonS3 _s3Client;
        //private readonly string processedMarker = ".processed";
        private readonly string processingMarker = ".processing";
        private const long LargeFileThreshold = 2L * 1024 * 1024 * 1024;
        private ConcurrentDictionary<string, byte> _activeUploads = new ConcurrentDictionary<string, byte>();

        // Настройки
        private readonly string _sourceFolder;
        private readonly string _archiveDirectory;
        private readonly uint _archiveClearedSeconds;
        private readonly bool _toArchivSourceFolder;
        private readonly string _bucketName;

        // Каналы данных
        private readonly Channel<FolderTask> _archivingChannel;
        private readonly Channel<UploadTask> _smallFilesChannel;
        private readonly Channel<UploadTask> _largeFilesChannel;

        public Worker(ILogger<Worker> logger, IOptions<AWSUploadSettings> options, IOptions<AWS> awsOptions)
        {
            _logger = logger;
            _sourceFolder = options.Value.SourceFolder;
            _archiveDirectory = options.Value.ArchiveDirectory;
            _archiveClearedSeconds = options.Value.ArchiveСlearedSeconds;
            _toArchivSourceFolder = options.Value.ToArchivSourceFolder;
            _bucketName = awsOptions.Value.BucketName;

            var clientConfig = new AmazonS3Config // Используем стандартный конфиг, если нет специфичной логики
            {
                ServiceURL = awsOptions.Value.S3Endpoint,
                AuthenticationRegion = awsOptions.Value.Region,
                ForcePathStyle = true
            };
            _s3Client = new AmazonS3Client(awsOptions.Value.AccessKey, awsOptions.Value.SecretKey, clientConfig);

            // Инициализация каналов
            // Unbounded - бесконечная очередь для сканера
            _archivingChannel = Channel.CreateUnbounded<FolderTask>();
           
            // Bounded - ограничиваем очередь загрузки, чтобы не забить память, если сеть медленная
            // Канал для мелочи: можно сделать буфер побольше (например, 200)
            _smallFilesChannel = Channel.CreateBounded<UploadTask>(new BoundedChannelOptions(200)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            // Канал для гигантов: буфер маленький (например, 2-5), чтобы не забить диск/память
            _largeFilesChannel = Channel.CreateBounded<UploadTask>(new BoundedChannelOptions(5)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"** {typeof(Worker).Name} SERVICE STARTED (Pipeline Mode) **");

            //Снимаем маркер из необработанных ранее папок
            await ClearProcessingAsync(stoppingToken);

            // Запускаем параллельные задачи (Consumers)
            var archivingTask = ProcessArchivingAsync(stoppingToken);

            // Можно запустить несколько загрузчиков параллельно, если нужно (например, 2 потока)
            var smallConsumer = ProcessUploadingAsync(_smallFilesChannel.Reader, stoppingToken, "SmallWorker");
           
            var largeConsumer = ProcessUploadingAsync(_largeFilesChannel.Reader, stoppingToken, "LargeWorker");

            // Основной цикл сканирования (Producer)
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await ScanFileSystemAsync(stoppingToken);

                    if (_archiveClearedSeconds != 0)
                        CleanArchive(_archiveClearedSeconds);

                    // Пауза перед следующим сканированием папки
                    await Task.Delay(5000, stoppingToken);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка в цикле сканирования");
                Environment.Exit(1);
            }
        }

        // --- 1. SCANNER (PRODUCER) ---
        private async Task ScanFileSystemAsync(CancellationToken ct)
        {
            if (!Directory.Exists(_sourceFolder)) return;

            var directories = Directory.GetDirectories(_sourceFolder);
            foreach (var subDirectory in directories)
            {
                // ШАГ А: Обработка файлов в ParentDir (Прямая загрузка)
                var filesInParent = Directory.EnumerateFiles(subDirectory);
                foreach (var file in filesInParent)
                {
                    // Пропускаем служебные файлы .notDelete
                    if (Path.GetExtension(file).ToLower() == ".notdelete") continue;
                    if (_activeUploads.ContainsKey(file)) continue;
                    if (IsMarked(file)) continue;

                    // Проверяем, что файл "устоялся" (не менялся > 30 сек)
                    if ((DateTime.Now - File.GetLastWriteTime(file)).TotalSeconds > 30)
                    {
                        if (_activeUploads.TryAdd(file, 0))
                        {
                            var fileInfo = new FileInfo(file);
                            var task = new UploadTask(file, subDirectory);

                            if (fileInfo.Length >= LargeFileThreshold)
                            {
                                // Отправляем в очередь для больших
                                await _largeFilesChannel.Writer.WriteAsync(task, ct);
                            }
                            else
                            {
                                // Отправляем в очередь для маленьких
                                await _smallFilesChannel.Writer.WriteAsync(task, ct);
                            }
                        }
                    }
                }

                // ШАГ Б: Обработка поддиректорий в ParentDir
                var parentDirName = new DirectoryInfo(subDirectory).Name;
                var subSubDirs = Directory.GetDirectories(subDirectory);

                foreach (var dir in subSubDirs)
                {
                    if (IsMarked(dir))
                        continue;
                    // Логика проверки стабильности файлов (Wait for files)
                    if (!IsDirectoryReady(dir))
                        continue;
                    
                    var processedDir = TryMarkProcessingDir(dir);

                    // Если папка готова, маркируем и отправляем в канал архивации
                    if(processedDir != null)
                        await _archivingChannel.Writer.WriteAsync(new FolderTask(processedDir, parentDirName), ct);
                    
                }
            }
        }

        private async Task ClearProcessingAsync(CancellationToken ct)
        {
            if (!Directory.Exists(_sourceFolder)) return;

            var directories = Directory.GetDirectories(_sourceFolder);
            foreach (var subDirectory in directories)
            {

                var parentDirName = new DirectoryInfo(subDirectory).Name;
                var subSubDirs = Directory.GetDirectories(subDirectory);

                foreach (var dir in subSubDirs)
                {
                    if (IsMarked(dir))
                    {
                        TryMark(dir, dir.Replace(processingMarker, string.Empty));
                    }
                }
            }
        }
        private bool IsDirectoryReady(string path)
        {
            var files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
            if (!files.Any()) return false;

            // Находим самый свежий файл
            var lastWriteTime = files.Select(x => File.GetLastWriteTime(x)).Max();

            // Проверка на "гостей из будущего"
            var timeDiff = (DateTime.Now - lastWriteTime).TotalSeconds;
            if (timeDiff < 0)
            {
                _logger.LogWarning($"Обнаружены файлы из будущего в {path}: {lastWriteTime}");
                return false;
            }

            // Условие задержки: файлы должны "отлежаться" 30 секунд (как в оригинале > 30)
            // Это гарантирует, что копирование завершено.
            if (timeDiff > 30)
            {
                return true;
            }

            return false;
        }

        private string TryMark(string original, string marked)
        {
            try
            {
                // Атомарное переименование
                Directory.Move(original, marked);
                return marked;
            }
            catch (IOException ex) when (ex is DirectoryNotFoundException)
            {
                // Папка исчезла - кто-то другой её удалил/переименовал
                return null;
            }
            catch (IOException ex) when (ex.Message.Contains("уже существует"))
            {
                // Уже переименовано другим процессом
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                // Нет прав
                return null;
            }
            catch (Exception)
            {
                // Другие ошибки
                return null;
            }
        }

        private string TryMarkProcessingDir(string originalDir)
        {
            string processingDir = originalDir + "_" + DateTime.Now.ToString("dd_MM_yy_hh_mm_ss")+ processingMarker;

            try
            {
                // Атомарное переименование
                Directory.Move(originalDir, processingDir);
                return processingDir;
            }
            catch (IOException ex) when (ex is DirectoryNotFoundException)
            {
                // Папка исчезла - кто-то другой её удалил/переименовал
                return null;
            }
            catch (IOException ex) when (ex.Message.Contains("уже существует"))
            {
                // Уже переименовано другим процессом
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                // Нет прав
                return null;
            }
            catch (Exception)
            {
                // Другие ошибки
                return null;
            }
        }
        private string TryMarkProcessed(string originalPath)
        {
            if (!originalPath.EndsWith(processingMarker))
                return originalPath;
            string processing = originalPath.Replace(processingMarker, "");

            try
            {
                // Атомарное переименование
                Directory.Move(originalPath, processing);
                return processing;
            }
            catch (IOException ex) when (ex is DirectoryNotFoundException)
            {
                // Папка исчезла - кто-то другой её удалил/переименовал
                return null;
            }
            catch (IOException ex) when (ex.Message.Contains("уже существует"))
            {
                // Уже переименовано другим процессом
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                // Нет прав
                return null;
            }
            catch (Exception)
            {
                // Другие ошибки
                return null;
            }
        }
        private bool IsMarked(string dirPath)
        {
            var dirName = Path.GetFileName(dirPath);
            return dirName.EndsWith(processingMarker);
        }

        // --- 2. ARCHIVER (CONSUMER 1) ---
        private async Task ProcessArchivingAsync(CancellationToken ct)
        {
            await foreach (var task in _archivingChannel.Reader.ReadAllAsync(ct))
            {
                try
                {
                    // Создаем .notDelete файл (сохраняем логику оригинала, хотя в каналах это менее критично)
                    GreateNotDeleteFile(Path.Combine(Path.GetDirectoryName(task.FolderPath), task.ParentDirName + ".notDelete"));

                    string zipPath = await FolderToZipAsync(task.FolderPath);

                    if (!string.IsNullOrEmpty(zipPath) && File.Exists(zipPath))
                    {
                        HandlePostZipCleanup(task);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Ошибка архивации папки {task.FolderPath}");
                }
            }
        }

        // --- 3. UPLOADER (CONSUMER 2) ---
        private async Task ProcessUploadingAsync(ChannelReader<UploadTask> reader, CancellationToken ct, string workerName)
        {
            await foreach (var task in reader.ReadAllAsync(ct))
            {
                try
                {
                    var request = new PutObjectRequest
                    {
                        BucketName = _bucketName,
                        Key = Path.GetFileName(task.ParentDirName) + "/" + Path.GetFileName(task.FilePath),
                        FilePath = task.FilePath
                    };

                    // === ИСПОЛЬЗОВАНИЕ ВАШИХ LEGACY МЕТОДОВ ===
                    AmazonWebServiceResponse response;
                    var fileInfo = new FileInfo(task.FilePath);

                    // Порог 2GB (как в оригинале)
                    if (fileInfo.Length < 2000 * (long)Math.Pow(2, 20))
                        response = await SendToS3Storage(request);
                    else
                        response = await TransferToS3StoregeAsync(request);
                    // ===========================================

                    if (response.HttpStatusCode == HttpStatusCode.OK)
                    {
                        // Успешная загрузка - Чистим или перемещаем исходники
                        HandlePostUploadCleanup(task);
                    }
                    else
                    {
                        _logger.LogError($"Ошибка S3 загрузки {task.FilePath}. Status: {response.HttpStatusCode}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Критическая ошибка загрузки файла {task.FilePath}");
                }
                finally
                {
                    _activeUploads.TryRemove(task.FilePath, out _); // Удаляем из кэша после любой обработки
                }
            }
        }

        private void HandlePostUploadCleanup(UploadTask task)
        {
            // Обработка самого ZIP файла (удалить или переместить)
            // Логика из оригинала: если _toArchivSourceFolder=true, то ZIP тоже мувится.
            // Но ZIP создается рядом с папкой. В оригинале была сложная логика с датами.
            if (_toArchivSourceFolder)
            {
                var dateFolder = Path.GetFileName(task.ParentDirName);
                var destFolder = Path.Combine(_archiveDirectory, dateFolder);
                Directory.CreateDirectory(destFolder);
                var destPath = Path.Combine(destFolder, Path.GetFileName(task.FilePath));

                // Проверка на существование и перемещение
                if (!File.Exists(destPath)) File.Move(task.FilePath, destPath);
            }
            else
            {
                DeleteFile(task.FilePath);
            }
        }

        private void HandlePostZipCleanup(FolderTask task)
        {
            // 1. Обработка исходной папки (удалить или переместить в архив)
            if (_toArchivSourceFolder)
            {
                MoveDir(task.FolderPath, Path.Combine(_archiveDirectory, task.ParentDirName));
            }
            else
            {
                Directory.Delete(task.FolderPath, true);
            }
        }

        // ==========================================
        // LEGACY METHODS (Оставлены как есть, с минимальными правками)
        // ==========================================

        private async Task<PutObjectResponse> SendToS3Storage(PutObjectRequest request)
        {
            try
            {
                // Добавлен ConfigureAwait для надежности
                return await _s3Client.PutObjectAsync(request).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                _logger.LogError($"Legacy Upload Error: {ex.Message}");
                return new PutObjectResponse { HttpStatusCode = HttpStatusCode.ExpectationFailed };
            }
            catch (Exception ex)
            {
                _logger.LogError($"Legacy Upload Error: {ex.Message}");
                return new PutObjectResponse { HttpStatusCode = HttpStatusCode.ExpectationFailed };
            }
        }

        private async Task<CompleteMultipartUploadResponse> TransferToS3StoregeAsync(PutObjectRequest request)
        {
            // Сохранена ваша логика ручного Multipart
            var fileToUpload = new FileInfo(request.FilePath);

            var initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = request.BucketName,
                Key = request.Key
            };
            var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);

            var partETags = new List<PartETag>();
            long partSize = 1000 * (long)Math.Pow(2, 20); // 1000 MB chunks
            long filePosition = 0;

            try
            {
                for (int i = 1; filePosition < fileToUpload.Length; i++)
                {
                    partSize = Math.Min(partSize, (fileToUpload.Length - filePosition));

                    // Важно: using stream внутри цикла безопасен только при последовательном выполнении (как здесь)
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

                        var uploadResponse = await _s3Client.UploadPartAsync(uploadRequest);
                        partETags.Add(new PartETag
                        {
                            PartNumber = i,
                            ETag = uploadResponse.ETag
                        });
                    }
                    filePosition += partSize;
                }

                var completeRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    UploadId = initiateResponse.UploadId,
                    PartETags = partETags
                };
                return await _s3Client.CompleteMultipartUploadAsync(completeRequest);
            }
            catch
            {
                // При ошибке хорошо бы отменять загрузку, чтобы не висели части в S3
                await _s3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    UploadId = initiateResponse.UploadId
                });
                throw;
            }
        }

        // --- ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (из вашего кода) ---
        private async Task<string> FolderToZipAsync(string folderPach)
        {
            string zipPach = string.Empty;
            // Убрал Task.Run "обертку", так как ZipFile блокирующий, 
            // но в контексте Channels это блокирует только Consumer поток, а не Scanner.
            // Для честной асинхронности лучше оставить Task.Run если ZipFile не умеет async
            await Task.Run(async () =>
            {
                // Небольшая задержка не нужна, если мы уже проверили IsDirectoryReady
                string zipFileName = folderPach.Replace(processingMarker, $".zip{processingMarker}");
                zipFileName = GetUniquFileName(zipFileName);
                ZipFile.CreateFromDirectory(folderPach, zipFileName, CompressionLevel.Fastest, false);
                zipPach =  TryMarkProcessed(zipFileName);
                
            });
            return zipPach;
        }

        private void MoveDir(string sourceDir, string destDir)
        {
            if (!Directory.Exists(sourceDir)) return;
            // Ваша логика перемещения
            destDir = GetUniquDirName(Path.Combine(destDir, Path.GetFileName(sourceDir).Replace(processingMarker,"")));
            if (Path.GetPathRoot(sourceDir) == Path.GetPathRoot(destDir))
            {
                Directory.Move(sourceDir, destDir);
            }
            else
            {
                CopyDirectory(sourceDir, destDir);
                Directory.Delete(sourceDir, true);
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

        private void CleanArchive(uint time)
        {
            // Ваша реализация CleanArchive...
            if (!Directory.Exists(_archiveDirectory)) return;
            foreach (var directory in Directory.GetDirectories(_archiveDirectory))
            {
                if ((DateTime.Now - Directory.GetCreationTime(directory)).TotalSeconds > time)
                    Directory.Delete(directory, true);
            }
        }

        private void GreateNotDeleteFile(string path)
        {
            if (!File.Exists(path)) using (File.Create(path)) ;
        }

        private void DeleteFile(string path)
        {
            // Ваша реализация DeleteFile...
            if (File.Exists(path))
            {
                File.SetAttributes(path, FileAttributes.Normal);
                File.Delete(path);
            }
        }

        private static string GetUniquFileName(string filename, string basename = "", int index = 0)
        {
            // Ваша реализация GetUniquFileName...
            if (basename == "") basename = Path.GetFileName(filename);
            string folderPath = Path.GetDirectoryName(filename);
            string uniqumFileName = filename;
            if (File.Exists(uniqumFileName))
            {
                index++;
                var splitedName = Path.GetFileName(basename).Split(".");
                // Упрощенная логика для краткости
                filename = Path.Combine(folderPath, Path.GetFileNameWithoutExtension(basename) + $" ({index})" + Path.GetExtension(basename));
                uniqumFileName = GetUniquFileName(filename, basename, index);
            }
            return uniqumFileName;
        }

        private static string GetUniquDirName(string filename, string basename = "", int index = 0)
        {
            // Ваша реализация GetUniquDirName...
            if (basename == "") basename = Path.GetFileName(filename);
            string folderPath = Path.GetDirectoryName(filename);
            string uniqumFileName = filename;
            if (Directory.Exists(uniqumFileName))
            {
                index++;
                filename = Path.Combine(folderPath, $"{basename}({index})");
                uniqumFileName = GetUniquDirName(filename, basename, index);
            }
            return uniqumFileName;
        }
    }
}