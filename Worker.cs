using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading.Channels;
using WorkerUnTarService;


namespace AWSUploadService
{
    // Модели для передачи данных между каналами
    public record FolderTask(string FolderPath, string ParentDirName);
    public record UploadTask(string FilePath, string ParentDirName);

    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IAmazonS3 _s3Client;
        private readonly TransferUtility _transferUtility;
        private readonly ConcurrentDictionary<string, byte> _activeUploads = new();

        private  string _targetBucketName;
        private  string _targetBucketPrefix;

        // Настройки
        private readonly string _sourceFolder;
        private readonly string _archiveDirectory;
        private readonly string _errorDirectory;
        private readonly uint _archiveClearedSeconds;
        private readonly bool _toArchivSourceFolder;
        private readonly string _bucketName;

        // Опции для расписания
        private readonly DateTime? _startTime;
        private readonly int _intervalHours;

        // Каналы данных
        private readonly Channel<FolderTask> _archivingChannel;
        private readonly Channel<UploadTask> _smallFilesChannel;
        private readonly Channel<UploadTask> _largeFilesChannel;

        // Константы
        private const string ProcessingMarker = ".processing";
        private const long LargeFileThreshold = 2L * 1024 * 1024 * 1024; // 2 GB
        private const int ChannelCapacity = 50;
        private const int LargeChannelCapacity = 5;
        private const int FileWriteDelaySeconds = 30;
        private const int S3ConnectionRetries = 5;
        private const int S3RetryDelayMs = 10000;
        private const int DefaultScanDelayMs = 5000;
        private const int S3PartSizeBytes = 200 * 1024 * 1024; // 200 MB

        public Worker(ILogger<Worker> logger, IOptions<AWSUploadSettings> options, IOptions<AWS> awsOptions)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            var settings = options.Value ?? throw new ArgumentNullException(nameof(options));
            var aws = awsOptions.Value ?? throw new ArgumentNullException(nameof(awsOptions));

            _sourceFolder = settings.SourceFolder;
            _archiveDirectory = settings.ArchiveDirectory;
            _errorDirectory = settings.ErrorFolder;
            _archiveClearedSeconds = settings.ArchiveСlearedSeconds;
            _toArchivSourceFolder = settings.ToArchivSourceFolder;
            _bucketName = aws.BucketName;

            //_startTime = settings.StartTime;
            //_intervalHours = settings.IntervalHours;

            ParseBucketName();

            var clientConfig = new AmazonS3Config
            {
                ServiceURL = aws.S3Endpoint,
                AuthenticationRegion = aws.Region,
                ForcePathStyle = true
            };
            _s3Client = new AmazonS3Client(aws.AccessKey, aws.SecretKey, clientConfig);
            _transferUtility = new TransferUtility(_s3Client);

            _archivingChannel = CreateBoundedChannel<FolderTask>(ChannelCapacity);
            _smallFilesChannel = CreateBoundedChannel<UploadTask>(ChannelCapacity);
            _largeFilesChannel = CreateBoundedChannel<UploadTask>(LargeChannelCapacity);
        }

        private void ParseBucketName()
        {
            var slashIndex = _bucketName.IndexOf('/');
            if (slashIndex > 0)
            {
                _targetBucketName = _bucketName.Substring(0, slashIndex);
                var prefix = _bucketName.Substring(slashIndex + 1);
                _targetBucketPrefix = string.IsNullOrEmpty(prefix) || prefix.EndsWith("/") ? prefix : prefix + "/";
            }
            else
            {
                _targetBucketName = _bucketName;
                _targetBucketPrefix = string.Empty;
            }
        }

        private static Channel<T> CreateBoundedChannel<T>(int capacity)
        {
            return Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"** {nameof(Worker)} SERVICE STARTED (Pipeline Mode) **");

            #region Валидация и инициализация
            if (!ValidateConfiguration())
            {
                _logger.LogCritical("Сервис остановлен из-за ошибок конфигурации.");
                return;
            }

            if (!await CheckS3ConnectionAsync(stoppingToken))
            {
                _logger.LogCritical("Не удалось подключиться к S3 после нескольких попыток. Сервис остановлен.");
                return;
            }

            _logger.LogInformation("Проверки пройдены. Запуск очистки временных файлов...");
            await ClearProcessingMarkersAsync(stoppingToken);
            #endregion

            #region Запуск consumer задач
            var archivingTask = ProcessArchivingAsync(stoppingToken);
            var smallUploadTask = ProcessUploadingAsync(_smallFilesChannel.Reader, stoppingToken, "SmallWorker");
            var largeUploadTask = ProcessUploadingAsync(_largeFilesChannel.Reader, stoppingToken, "LargeWorker");
            #endregion

            #region Обработка расписания
            await HandleSchedulingDelayAsync(stoppingToken);
            #endregion

            #region Основной цикл сканирования
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    DateTime startTime = DateTime.Now;
                    await ScanFileSystemAsync(stoppingToken);
                    if (_archiveClearedSeconds != 0)
                    {
                        CleanArchive(_archiveClearedSeconds);
                    }
                    await HandleScanDelayAsync(startTime, stoppingToken);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка в цикле сканирования");
                Environment.Exit(1);
            }
            finally
            {
                CompleteChannels();
                await Task.WhenAll(archivingTask, smallUploadTask, largeUploadTask);
            }
            #endregion
        }

        private async Task HandleSchedulingDelayAsync(CancellationToken ct)
        {
            if (_intervalHours == 0)
            {
                if (_startTime.HasValue)
                {
                    _logger.LogWarning("Директива StartTime проигнорирована, поскольку IntervalHours = 0. Работаем в бесконечном цикле.");
                }
                return;
            }

            if (!_startTime.HasValue)
            {
                throw new InvalidOperationException("StartTime должен быть указан, если IntervalHours != 0.");
            }

            var delay = _startTime.Value - DateTime.Now;
            if (delay > TimeSpan.Zero)
            {
                _logger.LogInformation($"Ожидание первого запуска до {_startTime.Value}...");
                await Task.Delay(delay, ct);
            }
            else
            {
                _logger.LogInformation("Время начала в прошлом — запуск immediate.");
            }
        }

        private async Task HandleScanDelayAsync(DateTime iterationStartTime, CancellationToken ct)
        {
            if (_intervalHours <= 0)
            {
                await Task.Delay(DefaultScanDelayMs, ct);
                return;
            }

            // 1. Проверяем, есть ли еще работа. 
            // Если работа еще идет, мы НЕ ЖДЕМ (как в while), а просто выходим,
            // чтобы основной цикл сразу перешел к расчету времени до следующего слота.
            bool stillWorking = !_activeUploads.IsEmpty || _archivingChannel.Reader.Count > 0;

            if (stillWorking)
            {
                _logger.LogWarning($"Итерация завершена со шлейфом активных задач. " +
                                   $"Загрузок: {_activeUploads.Count}, В очереди архива: {_archivingChannel.Reader.Count}. " +
                                   "Текущий цикл сканирования будет пропущен.");
            }

            // 2. Рассчитываем целевое время следующего запуска (строго от начала текущего)
            DateTime nextRunTime = iterationStartTime.AddHours(_intervalHours);
            TimeSpan delay = nextRunTime - DateTime.Now;

            if (delay > TimeSpan.Zero)
            {
                _logger.LogInformation($"Ожидание до следующего запланированного запуска: {nextRunTime}");
                await Task.Delay(delay, ct);
            }
            else
            {
                // Если работа (или ожидание шлейфа) заняла больше времени, чем сам интервал
                _logger.LogWarning($"Задержка превысила интервал. Следующая итерация начнется немедленно.");
            }
        }

        private void CompleteChannels()
        {
            _archivingChannel.Writer.Complete();
            _smallFilesChannel.Writer.Complete();
            _largeFilesChannel.Writer.Complete();
        }

        #region Scanner (Producer)
        private async Task ScanFileSystemAsync(CancellationToken ct)
        {
            if (!Directory.Exists(_sourceFolder)) return;

            var directories = Directory.GetDirectories(_sourceFolder);
            foreach (var parentDir in directories)
            {
                var parentDirName = Path.GetFileName(parentDir);

                // Обработка файлов в parentDir
                await ProcessFilesInDirectoryAsync(parentDir, parentDirName, ct);

                // Обработка поддиректорий
                await ProcessSubDirectoriesAsync(parentDir, parentDirName, ct);
            }
        }

        private async Task ProcessFilesInDirectoryAsync(string directory, string parentDirName, CancellationToken ct)
        {
            var files = Directory.EnumerateFiles(directory);
            foreach (var file in files)
            {
                if (Path.GetExtension(file).Equals(".notdelete", StringComparison.OrdinalIgnoreCase)) continue;
                if (_activeUploads.ContainsKey(file)) continue;
                if (IsMarkedAsProcessing(file)) continue;

                if ((DateTime.Now - File.GetLastWriteTime(file)).TotalSeconds <= FileWriteDelaySeconds) continue;

                if (_activeUploads.TryAdd(file, 0))
                {
                    var fileInfo = new FileInfo(file);
                    var task = new UploadTask(file, parentDirName);
                    var channel = fileInfo.Length >= LargeFileThreshold ? _largeFilesChannel : _smallFilesChannel;
                    await channel.Writer.WriteAsync(task, ct);
                }
            }
        }

        private async Task ProcessSubDirectoriesAsync(string parentDir, string parentDirName, CancellationToken ct)
        {
            string[] subDirs;
            try
            {
                subDirs = Directory.GetDirectories(parentDir);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Не удалось прочитать папку {parentDir}");
                return;
            }

            foreach (var subDir in subDirs)
            {
                if (IsMarkedAsProcessing(subDir)) continue;
                if (!IsDirectoryReady(subDir)) continue;

                var markedDir = MarkDirectoryAsProcessing(subDir);
                if (markedDir != null)
                {
                    await _archivingChannel.Writer.WriteAsync(new FolderTask(markedDir, parentDirName), ct);
                }
            }
        }
        #endregion

        #region Очистка маркеров обработки
        private async Task ClearProcessingMarkersAsync(CancellationToken ct)
        {
            if (!Directory.Exists(_sourceFolder)) return;

            var directories = Directory.GetDirectories(_sourceFolder);
            foreach (var parentDir in directories)
            {
                var subDirs = Directory.GetDirectories(parentDir);
                foreach (var subDir in subDirs)
                {
                    if (IsMarkedAsProcessing(subDir))
                    {
                        UnmarkDirectoryProcessing(subDir);
                    }
                }
            }
        }
        #endregion

        #region Archiver (Consumer)
        private async Task ProcessArchivingAsync(CancellationToken ct)
        {
            await foreach (var task in _archivingChannel.Reader.ReadAllAsync(ct))
            {
                try
                {
                    CreateNotDeleteFileIfNotExists(Path.Combine(Path.GetDirectoryName(task.FolderPath), $"{task.ParentDirName}.notDelete"));

                    var zipPath = await ArchiveFolderAsync(task.FolderPath);
                    if (!string.IsNullOrEmpty(zipPath) && File.Exists(zipPath))
                    {
                        HandlePostArchiveCleanup(task);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Ошибка архивации папки {task.FolderPath}");
                }
            }
        }

        private async Task<string> ArchiveFolderAsync(string folderPath)
        {
            return await Task.Run(() =>
            {
                var zipFileName = folderPath.Replace(ProcessingMarker, $".zip{ProcessingMarker}");
                zipFileName = GetUniqueFileName(zipFileName);

                ZipFile.CreateFromDirectory(folderPath, zipFileName, CompressionLevel.Fastest, false);

                return UnmarkFileProcessing(zipFileName);
            });
        }

        private void HandlePostArchiveCleanup(FolderTask task)
        {
            var cleanedPath = UnmarkDirectoryProcessing(task.FolderPath);
            if (cleanedPath == null) return;

            if (_toArchivSourceFolder)
            {
                MoveDirectory(cleanedPath, Path.Combine(_archiveDirectory, task.ParentDirName));
            }
            else
            {
                Directory.Delete(cleanedPath, true);
            }
        }
        #endregion

        #region Uploader (Consumer)
        private async Task ProcessUploadingAsync(ChannelReader<UploadTask> reader, CancellationToken ct, string workerName)
        {
            await foreach (var task in reader.ReadAllAsync(ct))
            {
                try
                {
                    var s3Key = BuildS3Key(task);
                    if (s3Key == null)
                    {
                        MoveToErrorDirectory(task.FilePath);
                        continue;
                    }

                    var uploadRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = _targetBucketName,
                        Key = s3Key,
                        FilePath = task.FilePath,
                        PartSize = S3PartSizeBytes,
                        DisablePayloadSigning = false,
                        AutoCloseStream = true
                    };

                    await _transferUtility.UploadAsync(uploadRequest, ct);

                    HandlePostUploadCleanup(task);
                }
                catch (AmazonS3Exception ex)
                {
                    _logger.LogError(ex, $"AWS ошибка при загрузке {task.FilePath}. Code: {ex.ErrorCode}");
                }
                catch (AmazonClientException ex)
                {
                    _logger.LogError(ex, $"Сетевая ошибка AWS при загрузке {task.FilePath}");
                }
                catch (IOException ex)
                {
                    _logger.LogError(ex, $"Ошибка доступа к файлу {task.FilePath}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Неизвестная ошибка загрузки {task.FilePath}. Файл помещен в ошибки.");
                    MoveToErrorDirectory(task.FilePath);
                }
                finally
                {
                    _activeUploads.TryRemove(task.FilePath, out _);
                }
            }
        }

        private string? BuildS3Key(UploadTask task)
        {
            try
            {
                var rawKey = _targetBucketPrefix + Path.GetFileName(task.ParentDirName) + "/" + Path.GetFileName(task.FilePath);
                return ValidateAndFixS3Key(rawKey);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Фатальная ошибка пути для {task.FilePath}");
                return null;
            }
        }

        private void HandlePostUploadCleanup(UploadTask task)
        {
            if (_toArchivSourceFolder)
            {
                var dateFolder = Path.GetFileName(task.ParentDirName);
                var destFolder = Path.Combine(_archiveDirectory, dateFolder);
                Directory.CreateDirectory(destFolder);

                var destPath = GetUniqueFileName(Path.Combine(destFolder, Path.GetFileName(task.FilePath)));
                File.Move(task.FilePath, destPath);
            }
            else
            {
                DeleteFileSafely(task.FilePath);
            }
        }
        #endregion

        #region Вспомогательные методы для файлов и директорий
        private bool IsDirectoryReady(string path)
        {
            var files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
            if (files.Length == 0) return false;

            var lastWriteTime = files.Max(File.GetLastWriteTime);
            var timeDiff = (DateTime.Now - lastWriteTime).TotalSeconds;

            if (timeDiff < 0)
            {
                _logger.LogWarning($"Обнаружены файлы из будущего в {path}: {lastWriteTime}");
                return false;
            }

            return timeDiff > FileWriteDelaySeconds;
        }

        private bool IsMarkedAsProcessing(string path)
        {
            return Path.GetFileName(path).EndsWith(ProcessingMarker);
        }

        private string? MarkDirectoryAsProcessing(string originalDir)
        {
            var timestamp = DateTime.Now.ToString("dd_MM_yy_hh_mm_ss");
            var newName = originalDir + "_" + timestamp + ProcessingMarker;
            return RenameDirectory(originalDir, newName) ? newName : null;
        }

        private string? UnmarkDirectoryProcessing(string originalPath)
        {
            if (!originalPath.EndsWith(ProcessingMarker)) return originalPath;

            var newName = originalPath.Replace(ProcessingMarker, string.Empty);
            return RenameDirectory(originalPath, newName) ? newName : null;
        }

        private string? UnmarkFileProcessing(string originalPath)
        {
            if (!originalPath.EndsWith(ProcessingMarker)) return originalPath;

            var newName = originalPath.Replace(ProcessingMarker, string.Empty);
            return RenameFile(originalPath, newName) ? newName : null;
        }

        private bool RenameDirectory(string source, string dest)
        {
            try
            {
                Directory.Move(source, dest);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, $"Ошибка переименования директории '{source}' в '{dest}'");
                return false;
            }
        }

        private bool RenameFile(string source, string dest)
        {
            try
            {
                File.Move(source, dest);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, $"Ошибка переименования файла '{source}' в '{dest}'");
                return false;
            }
        }

        private void MoveDirectory(string sourceDir, string destParent)
        {
            if (!Directory.Exists(sourceDir)) return;

            var destDir = GetUniqueDirectoryName(Path.Combine(destParent, Path.GetFileName(sourceDir)));

            var parentDir = Path.GetDirectoryName(destDir);
            if (!string.IsNullOrEmpty(parentDir))
            {
                Directory.CreateDirectory(parentDir);
            }

            if (Path.GetPathRoot(sourceDir) == Path.GetPathRoot(destDir))
            {
                Directory.Move(sourceDir, destDir);
            }
            else
            {
                CopyDirectoryRecursive(sourceDir, destDir);
                Directory.Delete(sourceDir, true);
            }
        }

        private static void CopyDirectoryRecursive(string sourceDir, string destDir)
        {
            Directory.CreateDirectory(destDir);

            foreach (var file in Directory.GetFiles(sourceDir))
            {
                var destFile = Path.Combine(destDir, Path.GetFileName(file));
                File.Copy(file, destFile, true);
            }

            foreach (var subDir in Directory.GetDirectories(sourceDir))
            {
                var destSubDir = Path.Combine(destDir, Path.GetFileName(subDir));
                CopyDirectoryRecursive(subDir, destSubDir);
            }
        }

        private void CleanArchive(uint retentionSeconds)
        {
            if (!Directory.Exists(_archiveDirectory)) return;

            foreach (var dir in Directory.GetDirectories(_archiveDirectory))
            {
                if ((DateTime.Now - Directory.GetCreationTime(dir)).TotalSeconds > retentionSeconds)
                {
                    Directory.Delete(dir, true);
                }
            }
        }

        private void CreateNotDeleteFileIfNotExists(string path)
        {
            if (!File.Exists(path))
            {
                File.Create(path).Dispose();
            }
        }

        private void DeleteFileSafely(string path)
        {
            if (File.Exists(path))
            {
                File.SetAttributes(path, FileAttributes.Normal);
                File.Delete(path);
            }
        }

        private static string GetUniqueFileName(string filePath)
        {
            var original = filePath;
            var count = 1;
            while (File.Exists(filePath))
            {
                var nameWithoutExt = Path.GetFileNameWithoutExtension(original);
                var ext = Path.GetExtension(original);
                filePath = Path.Combine(Path.GetDirectoryName(original)!, $"{nameWithoutExt} ({count}){ext}");
                count++;
            }
            return filePath;
        }

        private static string GetUniqueDirectoryName(string dirPath)
        {
            var original = dirPath;
            var count = 1;
            while (Directory.Exists(dirPath))
            {
                dirPath = $"{original} ({count})";
                count++;
            }
            return dirPath;
        }
        #endregion

        #region Валидация конфигурации и S3
        private bool ValidateConfiguration()
        {
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(_sourceFolder)) errors.Add("SourceFolder не указан.");
            if (string.IsNullOrWhiteSpace(_archiveDirectory)) errors.Add("ArchiveDirectory не указан.");
            if (string.IsNullOrWhiteSpace(_errorDirectory)) errors.Add("ErrorDirectory не указан.");
            if (string.IsNullOrWhiteSpace(_bucketName)) errors.Add("BucketName не указан.");

            if (!Directory.Exists(_sourceFolder))
                errors.Add($"Исходная папка не существует: {_sourceFolder}");

            CreateDirectoryIfNotExists(_archiveDirectory, errors, "архива");
            CreateDirectoryIfNotExists(_errorDirectory, errors, "ошибок");

            if (errors.Count > 0)
            {
                _logger.LogCritical("ОШИБКИ КОНФИГУРАЦИИ:\n" + string.Join("\n", errors));
                return false;
            }

            return true;
        }

        private void CreateDirectoryIfNotExists(string path, List<string> errors, string description)
        {
            if (string.IsNullOrWhiteSpace(path) || Directory.Exists(path)) return;

            try
            {
                Directory.CreateDirectory(path);
                _logger.LogInformation($"Создана директория {description}: {path}");
            }
            catch (Exception ex)
            {
                errors.Add($"Не удалось создать папку {description} {path}: {ex.Message}");
            }
        }

        private async Task<bool> CheckS3ConnectionAsync(CancellationToken ct)
        {
            bool s3Ready = false;
            int retryCount = 0;

            while (!ct.IsCancellationRequested && !s3Ready && retryCount < S3ConnectionRetries)
            {
                s3Ready = await EnsureBucketExistsAsync(ct);
                if (!s3Ready)
                {
                    retryCount++;
                    _logger.LogWarning($"Попытка подключения к S3 ({retryCount}/{S3ConnectionRetries}) не удалась. Пауза {S3RetryDelayMs / 1000} сек...");
                    await Task.Delay(S3RetryDelayMs, ct);
                }
            }

            return s3Ready;
        }

        private async Task<bool> EnsureBucketExistsAsync(CancellationToken ct)
        {
            try
            {
                _logger.LogInformation($"Проверка доступности бакета: {_targetBucketName}...");

                await _s3Client.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = _targetBucketName }, ct);

                _logger.LogInformation($"Бакет '{_targetBucketName}' подтвержден.");
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                _logger.LogWarning($"Бакет '{_targetBucketName}' не найден (404). Попытка создания...");
                return await CreateBucketAsync(ct);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.Forbidden)
            {
                _logger.LogCritical($"Ошибка 403: Нет прав доступа к бакету '{_targetBucketName}' или он не существует.");
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Ошибка при проверке бакета");
                return false;
            }
        }

        private async Task<bool> CreateBucketAsync(CancellationToken ct)
        {
            try
            {
                var putRequest = new PutBucketRequest
                {
                    BucketName = _targetBucketName,
                    UseClientRegion = true
                };
                await _s3Client.PutBucketAsync(putRequest, ct);
                _logger.LogInformation($"Бакет '{_targetBucketName}' успешно создан.");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Не удалось создать бакет");
                return false;
            }
        }

        private string ValidateAndFixS3Key(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("S3 Key cannot be empty.");

            // Замена обратных слешей на прямые
            var fixedKey = key.Replace("\\", "/");

            // Удаление двойных слешей
            while (fixedKey.Contains("//"))
            {
                fixedKey = fixedKey.Replace("//", "/");
            }

            // Удаление ведущего слеша
            fixedKey = fixedKey.TrimStart('/');

            // Замена недопустимых символов
            char[] invalidChars = { '\\', '{', '}', '^', '%', '`', ']', '[', '>', '<', '~', '#', '|' };
            foreach (var c in invalidChars)
            {
                if (fixedKey.Contains(c))
                {
                    _logger.LogWarning($"Символ '{c}' в ключе S3 '{fixedKey}' заменён на '_'");
                    fixedKey = fixedKey.Replace(c, '_');
                }
            }

            // Проверка длины
            if (Encoding.UTF8.GetByteCount(fixedKey) > 1024)
            {
                throw new InvalidOperationException($"Путь S3 слишком длинный: {fixedKey.Length} символов.");
            }

            return fixedKey;
        }
        #endregion

        #region Обработка ошибок (карантин)
        private void MoveToErrorDirectory(string filePath)
        {
            try
            {
                Directory.CreateDirectory(_errorDirectory);

                var fileName = Path.GetFileName(filePath);
                var destPath = GetUniqueFileName(Path.Combine(_errorDirectory, fileName));

                File.Move(filePath, destPath);
                _logger.LogWarning($"Файл перемещен в ошибку: {destPath}");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, $"НЕВОЗМОЖНО переместить файл в папку ошибок: {filePath}");
            }
        }
        #endregion
    }
}