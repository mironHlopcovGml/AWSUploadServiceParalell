using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerUnTarService
{
    public class AWSUploadSettings
    {
        public string ArchiveDirectory { get; set; }
        public string SourceFolder { get; set; }
        public uint ArchiveСlearedSeconds { get; set; }
        public bool ToArchivSourceFolder { get; set; }
        public bool OverwriteFiles { get; set; }
  
    }
}
