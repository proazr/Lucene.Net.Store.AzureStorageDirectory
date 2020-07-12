using System;
using System.IO;
using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store.AzureStorageDirectory
{
	public class AzureStorageIndexInput : IndexInput
	{
		private readonly PageBlobClient _pageBlobClient;

		private const int BufferSize = 1024 * 1024;
		private long _prevPageOffset;
		private long _currPageOffset;
		private long _currPagePosition;
		private long _blobLength = 0;
		private byte[] _currPage = null;

		public AzureStorageIndexInput(AzureStorageDirectory directory, string name)
			: base($"AzureStorageIndexInput(name={Path.Combine(directory.IndexFolder, name)})")
		{
			_pageBlobClient = directory.BlobContainerClient.GetPageBlobClient(name);
			_pageBlobClient.CreateIfNotExists(0);
			_blobLength = GetBlobLength();
		}

		public override long Length => _blobLength;

		public override long GetFilePointer()
		{
			return _currPagePosition;
		}

		public override byte ReadByte()
		{
			if (_blobLength == 0)
				return 0;

			if (_currPage == null || _prevPageOffset != GetCurrentPageOffset())
			{
				// download and cache the current page.
				_currPage ??= new byte[512];
				var range = new HttpRange(_currPageOffset, 512);
				BlobDownloadInfo download = _pageBlobClient.Download(range);
				download.Content.Read(_currPage, 0, 512);
			}

			var pos = _currPagePosition - _currPageOffset;
			var b = _currPage[pos];
			_currPagePosition += 1;
			return b;
		}

		public override void ReadBytes(byte[] b, int offset, int len)
		{
			// say offset = 200;
			int excess = 0;
			while (len > 0)
			{
				if (len > BufferSize)           // e.g. for len 800
				{
					excess = len % BufferSize;  // 288
					len -= excess;              // 512
				}

				BlobDownloadInfo download = _pageBlobClient.Download(new HttpRange(_currPagePosition, len));
				int read = download.Content.Read(b, offset, len); // 512 bytes downloaded

				_currPagePosition += len; // advance page blob position.
				SetCurrentPageOffset();

				len = excess;   // new len = 288
				offset += read; // new offset = 200 + 512
			}
		}

		private void SetCurrentPageOffset()
		{
			_prevPageOffset = _currPageOffset;
			_currPageOffset = _currPagePosition - (_currPagePosition % 512);
		}

		private long GetCurrentPageOffset()
		{
			return _currPagePosition - (_currPagePosition % 512);
		}

		public override void Seek(long pos)
		{
			if (pos > GetBlobLength())
			{
				throw new ArgumentException("pos cannot be greater than blob length.");
			}

			_currPagePosition = pos;
			SetCurrentPageOffset();
		}

		private long GetBlobLength(bool refetch = false)
		{
			if (refetch || _blobLength <= 0)
			{
				if (_pageBlobClient.Exists())
				{
					BlobProperties props = _pageBlobClient.GetProperties();
					_blobLength = props.Metadata.TryGetValue("actuallength", out string actualLength)
						? long.Parse(actualLength)
						: props.ContentLength;
				}
			}

			return _blobLength;
		}

		protected override void Dispose(bool disposing)
		{
			// nothing to dispose.
		}
	}
}
