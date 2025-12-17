using Amazon.S3;

namespace WorkerUnTarService.Models
{
    public class MyAmazonS3Config: AmazonS3Config
    {
        public string BucketName {  get; set; }
    }
}
