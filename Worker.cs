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
using System.Threading.Channels;
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
        private readonly TransferUtility _transferUtility;
        private readonly string _processingMarker = ".processing";
        private const long LargeFileThreshold = 2L * 1024 * 1024 * 1024;
        private readonly ConcurrentDictionary<string, byte> _activeUploads = new ConcurrentDictionary<string, byte>();

        private readonly string _targetBucketName;
        private readonly string _targetBucketPrefix;

        // Настройки
        private readonly string _sourceFolder;
        private readonly string _archiveDirectory;
        private readonly string _errorDirectory;
        private readonly uint _archiveClearedSeconds;
        private readonly bool _toArchivSourceFolder;
        private readonly string _bucketName;
        // Опции для расписания
        private readonly DateTime? _startTime; // Время первого запуска
        private readonly int _intervalHours;   // Периодичность в часах

        // Каналы данных
        private readonly Channel<FolderTask> _archivingChannel;
        private readonly Channel<UploadTask> _smallFilesChannel;
        private readonly Channel<UploadTask> _largeFilesChannel;

        public Worker(ILogger<Worker> logger, IOptions<AWSUploadSettings> options, IOptions<AWS> awsOptions)
        {
            _logger = logger;
            _sourceFolder = options.Value.SourceFolder;
            _archiveDirectory = options.Value.ArchiveDirectory;
            _errorDirectory = options.Value.ErrorFolder;
            _archiveClearedSeconds = options.Value.ArchiveСlearedSeconds;
            _toArchivSourceFolder = options.Value.ToArchivSourceFolder;
            _bucketName = awsOptions.Value.BucketName;
            // Чтение опций расписания
            _startTime = options.Value.StartTime; 
            _intervalHours = options.Value.IntervalHours; 

            // ПАРСИНГ БАКЕТА В КОНСТРУКТОРЕ
            var slashIndex = _bucketName.IndexOf('/');
            if (slashIndex > 0)
            {
                _targetBucketName = _bucketName.Substring(0, slashIndex);
                var p = _bucketName.Substring(slashIndex + 1);
                _targetBucketPrefix = !string.IsNullOrEmpty(p) && !p.EndsWith("/") ? p + "/" : p;
            }
            else
            {
                _targetBucketName = _bucketName;
                _targetBucketPrefix = string.Empty;
            }

            var clientConfig = new AmazonS3Config
            {
                ServiceURL = awsOptions.Value.S3Endpoint,
                AuthenticationRegion = awsOptions.Value.Region,
                ForcePathStyle = true
            };
            _s3Client = new AmazonS3Client(awsOptions.Value.AccessKey, awsOptions.Value.SecretKey, clientConfig);
            _transferUtility = new TransferUtility(_s3Client);

            // Инициализация каналов
            _archivingChannel = Channel.CreateBounded<FolderTask>(new BoundedChannelOptions(50)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
            _smallFilesChannel = Channel.CreateBounded<UploadTask>(new BoundedChannelOptions(50)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
            _largeFilesChannel = Channel.CreateBounded<UploadTask>(new BoundedChannelOptions(5)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"** {typeof(Worker).Name} SERVICE STARTED (Pipeline Mode) **");

            // --- ЭТАП 1: ВАЛИДАЦИЯ КОНФИГУРАЦИИ ---
            if (!ValidateConfiguration())
            {
                _logger.LogCritical("Сервис остановлен из-за ошибок конфигурации.");
                // В .NET BackgroundService выход из ExecuteAsync означает остановку сервиса
                return;
            }

            // --- ЭТАП 2: ПРОВЕРКА СВЯЗИ С S3 (Retry Policy) ---
            // Делаем несколько попыток на случай, если сеть поднимается медленнее сервиса
            bool s3Ready = false;
            int retryCount = 0;
            while (!stoppingToken.IsCancellationRequested && !s3Ready && retryCount < 5)
            {
                s3Ready = await EnsureBucketExistsAsync(stoppingToken);
                if (!s3Ready)
                {
                    retryCount++;
                    _logger.LogWarning($"Попытка подключения к S3 ({retryCount}/5) не удалась. Пауза 10 сек...");
                    await Task.Delay(10000, stoppingToken);
                }
            }

            if (!s3Ready)
            {
                _logger.LogCritical("Не удалось подключиться к S3 после нескольких попыток. Сервис остановлен.");
                return;
            }

            // --- ЭТАП 3: ОЧИСТКА "ЗАВИСШИХ" ФАЙЛОВ ---
            _logger.LogInformation("Проверки пройдены. Запуск очистки временных файлов...");
            await ClearProcessingAsync(stoppingToken);

            // --- ЭТАП 4: ЗАПУСК CONSUMERS ---
            var archivingTask = ProcessArchivingAsync(stoppingToken);
            var smallConsumer = ProcessUploadingAsync(_smallFilesChannel.Reader, stoppingToken, "SmallWorker");
            var largeConsumer = ProcessUploadingAsync(_largeFilesChannel.Reader, stoppingToken, "LargeWorker");

            // --- ЭТАП 5: ОБРАБОТКА РАСПИСАНИЯ ---
            if (_intervalHours == 0)
            {
                if (_startTime.HasValue)
                {
                    _logger.LogWarning("Директива StartTime проигнорирована, поскольку IntervalHours = 0. Работаем в бесконечном цикле.");
                }
                // Нет ожидания первого запуска, сразу цикл
            }
            else
            {
                if (!_startTime.HasValue)
                {
                    throw new InvalidOperationException("StartTime должен быть указан, если IntervalHours != 0.");
                }

                // Ожидание первого запуска
                var delay = _startTime.Value - DateTime.Now;
                if (delay > TimeSpan.Zero)
                {
                    _logger.LogInformation($"Ожидание первого запуска до {_startTime.Value}...");
                    await Task.Delay(delay, stoppingToken);
                }
                else
                {
                    _logger.LogInformation("Время начала в прошлом — запуск immediate.");
                }
            }

            // --- ЭТАП 6: ОСНОВНОЙ ЦИКЛ ---
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await ScanFileSystemAsync(stoppingToken);
                    if (_archiveClearedSeconds != 0)
                    {
                        CleanArchive(_archiveClearedSeconds);
                    }

                    // Задержка и логирование следующего запуска
                    if (_intervalHours > 0)
                    {
                        var nextRun = DateTime.Now + TimeSpan.FromHours(_intervalHours);
                        _logger.LogInformation($"Следующий запуск запланирован на {nextRun}.");
                        var delayMs = _intervalHours * 60 * 60 * 1000; // Часы в миллисекунды
                        await Task.Delay(delayMs, stoppingToken);
                    }
                    else
                    {
                        // Обычная пауза 5 секунд
                        await Task.Delay(5000, stoppingToken);
                    }
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
                // Завершаем каналы
                _archivingChannel.Writer.Complete();
                _smallFilesChannel.Writer.Complete();
                _largeFilesChannel.Writer.Complete();

                // Ждём завершения consumers
                await Task.WhenAll(archivingTask, smallConsumer, largeConsumer);
            }
        }
       
        // --- 1. SCANNER (PRODUCER) ---
        private async Task ScanFileSystemAsync(CancellationToken ct)
        {
            if (!Directory.Exists(_sourceFolder)) return;

            var directories = Directory.GetDirectories(_sourceFolder);
            foreach (var subDirectory in directories)
            {
                // Обработка файлов в ParentDir (Прямая загрузка)
                var filesInParent = Directory.EnumerateFiles(subDirectory);
                foreach (var file in filesInParent)
                {
                    if (Path.GetExtension(file).ToLower() == ".notdelete") continue;
                    if (_activeUploads.ContainsKey(file)) continue;
                    if (IsMarked(file)) continue;

                    if ((DateTime.Now - File.GetLastWriteTime(file)).TotalSeconds > 30)
                    {
                        if (_activeUploads.TryAdd(file, 0))
                        {
                            var fileInfo = new FileInfo(file);
                            var task = new UploadTask(file, subDirectory);
                            var channel = fileInfo.Length >= LargeFileThreshold ? _largeFilesChannel : _smallFilesChannel;
                            await channel.Writer.WriteAsync(task, ct);
                        }
                    }
                }

                // Обработка поддиректорий в ParentDir
                var parentDirName = new DirectoryInfo(subDirectory).Name;
                string[] subSubDirs;
                try
                {
                    subSubDirs = Directory.GetDirectories(subDirectory);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Не удалось прочитать папку {subDirectory}: {ex.Message}");
                    continue;
                }
                foreach (var dir in subSubDirs)
                {
                    if (IsMarked(dir)) continue;
                    if (!IsDirectoryReady(dir)) continue;

                    var processedDir = TryMarkProcessingDir(dir);
                    if (processedDir != null)
                    {
                        await _archivingChannel.Writer.WriteAsync(new FolderTask(processedDir, parentDirName), ct);
                    }
                }
            }
        }

        private async Task ClearProcessingAsync(CancellationToken ct)
        {
            if (!Directory.Exists(_sourceFolder)) return;

            var directories = Directory.GetDirectories(_sourceFolder);
            foreach (var subDirectory in directories)
            {
                var subSubDirs = Directory.GetDirectories(subDirectory);
                foreach (var dir in subSubDirs)
                {
                    if (IsMarked(dir))
                    {
                        var newName = dir.Replace(_processingMarker, string.Empty);
                        TryRenameDir(dir, newName);
                    }
                }
            }
        }

        private bool IsDirectoryReady(string path)
        {
            var files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
            if (!files.Any()) return false;

            var lastWriteTime = files.Select(File.GetLastWriteTime).Max();
            var timeDiff = (DateTime.Now - lastWriteTime).TotalSeconds;

            if (timeDiff < 0)
            {
                _logger.LogWarning($"Обнаружены файлы из будущего в {path}: {lastWriteTime}");
                return false;
            }

            return timeDiff > 30;
        }

        private bool IsMarked(string path)
        {
            var name = Path.GetFileName(path);
            return name.EndsWith(_processingMarker);
        }

        private string TryMarkProcessingDir(string originalDir)
        {
            var timestamp = DateTime.Now.ToString("dd_MM_yy_hh_mm_ss");
            var newName = originalDir + "_" + timestamp + _processingMarker;
            return TryRenameDir(originalDir, newName) ? newName : null;
        }

        private string TryMarkProcessed(string originalPath)
        {
            if (!originalPath.EndsWith(_processingMarker)) return originalPath;

            var newName = originalPath.Replace(_processingMarker, string.Empty);
            return TryRenameDir(originalPath, newName) ? newName : null;
        }

        private bool TryRenameDir(string source, string dest)
        {
            try
            {
                Directory.Move(source, dest);
                return true;
            }
            catch (DirectoryNotFoundException) { }
            catch (IOException ex) when (ex.Message.Contains("уже существует")) { }
            catch (UnauthorizedAccessException) { }
            catch (Exception ex) 
            {
                _logger.LogDebug(ex, $"Ошибка переименования '{source}' в '{dest}'"); 
                return false;
            }
            return false;
        }

        // --- 2. ARCHIVER (CONSUMER 1) ---
        private async Task ProcessArchivingAsync(CancellationToken ct)
        {
            await foreach (var task in _archivingChannel.Reader.ReadAllAsync(ct))
            {
                try
                {
                    CreateNotDeleteFile(Path.Combine(Path.GetDirectoryName(task.FolderPath), task.ParentDirName + ".notDelete"));
                    var zipPath = await FolderToZipAsync(task.FolderPath);
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
                    // 1. Валидация пути S3
                    string safeKey;
                    try
                    {
                        string rawKey = _targetBucketPrefix + Path.GetFileName(task.ParentDirName) + "/" + Path.GetFileName(task.FilePath);
                        safeKey = ValidateAndFixS3Key(rawKey);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Фатальная ошибка пути для {task.FilePath}: {ex.Message}");
                        MoveToErrorDirectory(task.FilePath); // Сразу в карантин
                        continue;
                    }

                    var uploadRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = _targetBucketName,
                        Key = safeKey,
                        FilePath = task.FilePath,
                        PartSize = 200 * 1024 * 1024,
                        DisablePayloadSigning = false,
                        AutoCloseStream = true
                    };
                    await _transferUtility.UploadAsync(uploadRequest, ct);
                    HandlePostUploadCleanup(task);
                }
                catch (Amazon.Runtime.AmazonServiceException ex)
                {
                    // Сервер S3 ответил ошибкой (например, 500 или 503 Slow Down)
                    _logger.LogError($"AWS Server Error при загрузке {task.FilePath}: {ex.Message}. Code: {ex.ErrorCode}");
                }
                catch (Amazon.Runtime.AmazonClientException ex)
                {
                    // Ошибка сети. Вот тут ловится HttpErrorResponseException.
                    _logger.LogError($"Сетевая ошибка AWS при загрузке {task.FilePath}: {ex.Message}");
                    // Здесь можно не удалять файл из _activeUploads, чтобы попробовать снова в следующем цикле сканирования, 
                    // но у вас логика построена так, что он останется в папке и подхватится снова.
                }
                catch (IOException ex)
                {
                    _logger.LogError($"Ошибка доступа к файлу {task.FilePath}: {ex.Message}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Неизвестная ошибка загрузки {task.FilePath}. Файл помещен в ошибки.");
                    MoveToErrorDirectory(task.FilePath);
                }
                finally
                {
                    // Всегда освобождаем маркер занятости
                    _activeUploads.TryRemove(task.FilePath, out _);
                }
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
                if (!File.Exists(destPath))
                {
                    File.Move(task.FilePath, destPath);
                }
            }
            else
            {
                DeleteFile(task.FilePath);
            }
        }

        private void HandlePostZipCleanup(FolderTask task)
        {
            if (_toArchivSourceFolder)
            {
                MoveDir(task.FolderPath, Path.Combine(_archiveDirectory, task.ParentDirName));
            }
            else
            {
                Directory.Delete(task.FolderPath, true);
            }
        }

        // --- ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ---
        private async Task<string> FolderToZipAsync(string folderPath)
        {
            string zipPath = string.Empty;
            await Task.Run(() =>
            {
                var zipFileName = folderPath.Replace(_processingMarker, $".zip{_processingMarker}");
                zipFileName = GetUniqueFileName(zipFileName);
                ZipFile.CreateFromDirectory(folderPath, zipFileName, CompressionLevel.Fastest, false);
                zipPath = TryMarkProcessed(zipFileName);
            });
            return zipPath;
        }

        private void MoveDir(string sourceDir, string destDir)
        {
            if (!Directory.Exists(sourceDir)) return;

            destDir = GetUniqueDirName(Path.Combine(destDir, Path.GetFileName(sourceDir).Replace(_processingMarker, string.Empty)));

            // Создаём родительскую директорию для destDir, если нужно
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
                CopyDirectory(sourceDir, destDir);
                Directory.Delete(sourceDir, true);
            }
        }

        public static void CopyDirectory(string sourceDir, string destDir)
        {
            if (!Directory.Exists(destDir))
            {
                Directory.CreateDirectory(destDir);
            }

            foreach (var file in Directory.GetFiles(sourceDir))
            {
                var destFile = Path.Combine(destDir, Path.GetFileName(file));
                File.Copy(file, destFile, true);
            }

            foreach (var dir in Directory.GetDirectories(sourceDir))
            {
                var destSubDir = Path.Combine(destDir, Path.GetFileName(dir));
                CopyDirectory(dir, destSubDir);
            }
        }

        private void CleanArchive(uint time)
        {
            if (!Directory.Exists(_archiveDirectory)) return;

            foreach (var directory in Directory.GetDirectories(_archiveDirectory))
            {
                if ((DateTime.Now - Directory.GetCreationTime(directory)).TotalSeconds > time)
                {
                    Directory.Delete(directory, true);
                }
            }
        }

        private void CreateNotDeleteFile(string path)
        {
            if (!File.Exists(path))
            {
                using (File.Create(path)) { }
            }
        }

        private void DeleteFile(string path)
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
                filePath = Path.Combine(Path.GetDirectoryName(original), $"{nameWithoutExt} ({count}){ext}");
                count++;
            }
            return filePath;
        }

        private static string GetUniqueDirName(string dirPath)
        {
            var original = dirPath;
            var count = 1;
            while (Directory.Exists(dirPath))
            {
                dirPath = Path.Combine(Path.GetDirectoryName(original), $"{Path.GetFileName(original)} ({count})");
                count++;
            }
            return dirPath;
        }





        //////////Валидация 
        // --- PRE-FLIGHT CHECKS ---

        private bool ValidateConfiguration()
        {
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(_sourceFolder)) errors.Add("SourceFolder не указан.");
            if (string.IsNullOrWhiteSpace(_archiveDirectory)) errors.Add("ArchiveDirectory не указан.");
            if (string.IsNullOrWhiteSpace(_errorDirectory)) errors.Add("ErrorDirectory не указан.");
            if (string.IsNullOrWhiteSpace(_bucketName)) errors.Add("BucketName не указан.");

            // Проверка существования локальных папок
            if (!Directory.Exists(_sourceFolder))
                errors.Add($"Исходная папка не существует: {_sourceFolder}");

            // Папку архива можно создать, если нет
            if (!string.IsNullOrWhiteSpace(_archiveDirectory) && !Directory.Exists(_archiveDirectory))
            {
                try
                {
                    Directory.CreateDirectory(_archiveDirectory);
                }
                catch (Exception ex)
                {
                    errors.Add($"Не удалось создать папку архива {_archiveDirectory}: {ex.Message}");
                }
            }
            // 3. Проверка/Создание папки ошибок (карантина)
            if (!string.IsNullOrWhiteSpace(_errorDirectory) && !Directory.Exists(_errorDirectory))
            {
                try
                {
                    Directory.CreateDirectory(_errorDirectory);
                    _logger.LogInformation($"Создана директория ошибок: {_errorDirectory}");
                }
                catch (Exception ex)
                {
                    errors.Add($"Не удалось создать папку ошибок {_errorDirectory}: {ex.Message}");
                }
            }
            if (errors.Any())
            {
                _logger.LogCritical("ОШИБКИ КОНФИГУРАЦИИ:\n" + string.Join("\n", errors));
                return false;
            }

            return true;
        }

        private async Task<bool> EnsureBucketExistsAsync(CancellationToken ct)
        {
            try
            {
                _logger.LogInformation($"Проверка доступности бакета: {_targetBucketName}...");

                // Метод GetBucketLocationAsync — самый надежный способ проверить реальное наличие
                await _s3Client.GetBucketLocationAsync(new GetBucketLocationRequest
                {
                    BucketName = _targetBucketName
                }, ct);

                _logger.LogInformation($"Бакет '{_targetBucketName}' подтвержден.");
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Бакет точно не найден — пытаемся создать
                _logger.LogWarning($"Бакет '{_targetBucketName}' не найден (404). Попытка создания...");
                return await CreateBucketAsync(ct);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
            {
                _logger.LogCritical($"Ошибка 403: Нет прав доступа к бакету '{_targetBucketName}' или он не существует.");
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Ошибка при проверке бакета: {ex.Message}");
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
                _logger.LogCritical($"Не удалось создать бакет: {ex.Message}");
                return false;
            }
        }

        private string ValidateAndFixS3Key(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("S3 Key cannot be empty.");

            // 1. Заменяем обратные слеши (Windows style) на прямые
            string fixedKey = key.Replace("\\", "/");

            // 2. Удаляем двойные слеши, которые создают "пустые" папки
            while (fixedKey.Contains("//"))
            {
                fixedKey = fixedKey.Replace("//", "/");
            }

            // 3. Удаляем ведущий слеш (S3 ключи обычно начинаются без него)
            fixedKey = fixedKey.TrimStart('/');

            // 4. Проверка на недопустимые символы (ASCII управляющие символы и др.)
            // Рекомендуется избегать: \ { } ^ % ` ] [ > < ~ # |
            char[] invalidChars = { '\\', '{', '}', '^', '%', '`', ']', '[', '>', '<', '~', '#', '|' };
            foreach (var c in invalidChars)
            {
                if (fixedKey.Contains(c))
                {
                    _logger.LogWarning($"Символ '{c}' в ключе S3 '{fixedKey}' может вызвать проблемы в некоторых клиентах. Попытка заменить '{fixedKey}' на '_'");
                    // Можно либо заменить на '_', либо оставить как есть, если ваш S3 провайдер их поддерживает
                     fixedKey = fixedKey.Replace(c, '_'); 
                }
            }

            // 5. Ограничение длины (S3 поддерживает до 1024 байт)
            if (System.Text.Encoding.UTF8.GetByteCount(fixedKey) > 1024)
            {
                throw new Exception($"Путь S3 слишком длинный: {fixedKey.Length} символов. Файл перемещен в директорию с архивами или удален.");
            }

            return fixedKey;
        }

        private void MoveToErrorDirectory(string filePath)
        {
            try
            {
                if (!Directory.Exists(_errorDirectory))
                    Directory.CreateDirectory(_errorDirectory);

                string fileName = Path.GetFileName(filePath);
                string destPath = Path.Combine(_errorDirectory, fileName);

                // Гарантируем уникальность имени в папке ошибок
                string uniqueDestPath = GetUniqueFileName(destPath);

                File.Move(filePath, uniqueDestPath);
                _logger.LogWarning($"Файл перемещен в КАРАНТИН: {uniqueDestPath}");
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"НЕВОЗМОЖНО переместить файл в папку ошибок: {filePath}. Ошибка: {ex.Message}");
            }
        }
    }
}