using System.IO;
using Azure;
using Azure.Storage.Blobs.Models;

namespace Lucene.Net.Store.AzureStorageDirectory
{
	public class AzureStorageLock : Lock
	{
		private bool _disposed;
		private readonly string _lockName;
		private readonly AzureStorageDirectory _directory;

		public AzureStorageLock(AzureStorageDirectory directory, string lockName)
		{
			_lockName = lockName;
			_directory = directory;
		}

		public override bool IsLocked()
		{
			return _directory.BlobExists(_lockName);
		}

		public override bool Obtain()
		{
			var blobClient = _directory.BlobContainerClient.GetBlobClient(_lockName);

			if (blobClient.Exists())
			{
				return false;
			}

			using (var memoryStream = new MemoryStream())
			using (var streamWriter = new StreamWriter(memoryStream))
			{
				streamWriter.Write(_lockName);
				Response<BlobContentInfo> response = blobClient.Upload(memoryStream);
			}

			return true;
		}

		protected override void Dispose(bool disposing)
		{
			if (!_disposed)
			{

				if (disposing)
				{
					var blobClient = _directory.BlobContainerClient.GetBlobClient(_lockName);
					Response<bool> response = blobClient.DeleteIfExists();
				}

				_disposed = true;
			}
		}
	}
}
