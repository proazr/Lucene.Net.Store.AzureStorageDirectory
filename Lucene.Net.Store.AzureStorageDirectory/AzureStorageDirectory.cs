using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Lucene.Net.Store.AzureStorageDirectory
{
	public class AzureStorageDirectory : Directory
	{
		private LockFactory _lockFactory;

		// TODO: make options
		public AzureStorageDirectory(string connectionString, string containerName, string indexFolder = null)
		{
			_lockFactory = new AzureStorageLockFactory(this);
			BlobServiceClient = new BlobServiceClient(connectionString);
			BlobContainerClient = BlobServiceClient.GetBlobContainerClient(containerName);
			IndexFolder = string.IsNullOrEmpty(indexFolder) ? string.Empty : indexFolder.Trim('/');

			EnsureContainerExists();
		}

		internal string IndexFolder { get; }

		internal BlobServiceClient BlobServiceClient { get; }

		internal BlobContainerClient BlobContainerClient { get; }

		private string GetIndexBlobName(string name) => Path.Combine(IndexFolder, name);

		public override LockFactory LockFactory => _lockFactory;

		private void EnsureContainerExists()
		{
			if (!BlobContainerClient.Exists())
			{
				BlobContainerInfo response = BlobContainerClient.Create();
			}
		}

		internal bool BlobExists(string blobName)
		{
			var blobClient = BlobContainerClient.GetBlobClient(blobName);
			return blobClient.Exists();
		}

		private ISet<string> GetBlobNames(int? segmentSize = null, string continuationToken = null, string prefix = null)
		{
			var blobNames = new HashSet<string>();
			var blobPages = BlobContainerClient.GetBlobs().AsPages(continuationToken, segmentSize);

			do
			{
				foreach (Page<BlobItem> blobPage in blobPages)
				{
					foreach (var blobItem in blobPage.Values)
					{
						// TODO: get properties as well?
						blobNames.Add(blobItem.Name);
					}

					continuationToken = blobPage.ContinuationToken;
				}
			} while (!string.IsNullOrEmpty(continuationToken));

			return blobNames;
		}

		public override void ClearLock(string name)
		{
			LockFactory.ClearLock(name);
		}

		public override IndexOutput CreateOutput(string name, IOContext context)
		{
			var blobName = GetIndexBlobName(name);
			return new AzureStorageIndexOutput(this, blobName);
		}

		public override void DeleteFile(string name)
		{
			var blobClient = BlobContainerClient.GetBlobClient(name);
			bool response = blobClient.DeleteIfExists();
			// TODO: verify response.
		}

		[Obsolete]
		public override bool FileExists(string name)
		{
			var blobClient = BlobContainerClient.GetBlobClient(name);
			return blobClient.Exists();
		}

		public override long FileLength(string name)
		{
			var blobClient = BlobContainerClient.GetBlobClient(name);

			if (!blobClient.Exists())
			{
				return -1;
			}

			Response<BlobProperties> response = blobClient.GetProperties();
			var length = response.Value.ContentLength;
			return length;
		}

		public override string[] ListAll()
		{
			return GetBlobNames(prefix: IndexFolder).ToArray();
		}

		public override Lock MakeLock(string name)
		{
			return LockFactory.MakeLock(name);
		}

		public override IndexInput OpenInput(string name, IOContext context)
		{
			var blobName = GetIndexBlobName(name);
			return new AzureStorageIndexInput(this, blobName);
		}

		public override void SetLockFactory(LockFactory lockFactory)
		{
			Debug.Assert(lockFactory != null);

			_lockFactory = lockFactory;
			_lockFactory.LockPrefix = this.GetLockID();
		}

		public override void Sync(ICollection<string> names)
		{
			// LUCENENET specific: No such thing as "stale files" in .NET, since Flush(true) writes everything to disk before
			// our FileStream is disposed. Therefore, there is nothing else to do in this method.
		}

		protected override void Dispose(bool disposing)
		{
			// nothing to dispose.
		}
	}
}
