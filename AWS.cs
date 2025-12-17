using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerUnTarService
{
    public  class AWS
    {
        public string BucketName { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string S3Endpoint { get; set; }
        public string Region { get; set; }

    }
}
