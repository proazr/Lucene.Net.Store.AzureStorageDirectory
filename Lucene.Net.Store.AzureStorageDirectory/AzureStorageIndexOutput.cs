using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store.AzureStorageDirectory
{
	public class AzureStorageIndexOutput : IndexOutput
	{
		private const int PageSize = 512;
		private const int BufferSize = 1024 * 64;
		private const int MaxLength = 1024 * 1024 * 4; // page blob client caps the max bytes that can be written per transaction at 4 MB.

		private byte[] _buffer;
		private int _bufferPosition;
		private long _lastPageOffset;
		private long _currPageOffset;   // _currPagePosition - (_currPagePosition % 512).
		private long _currPagePosition; // NOTE: this will always be a multiple of 512.
		private readonly PageBlobClient _pageBlobClient;
		private readonly IDictionary<string, string> _metadata = new Dictionary<string, string>();

		public AzureStorageIndexOutput(AzureStorageDirectory directory, string name)
		{
			_pageBlobClient = directory.BlobContainerClient.GetPageBlobClient(name);
			_pageBlobClient.CreateIfNotExists(0);
		}

		public override long Checksum { get; }

		public virtual void Reset()
		{
			_buffer = null;
			_bufferPosition = -1;
		}

		public override long GetFilePointer() => _currPagePosition;

		[Obsolete]
		public override void Seek(long pos)
		{
			throw new NotImplementedException();
		}

		public override void WriteByte(byte b)
		{
			// lazy create buffer.
			_buffer ??= new byte[BufferSize];

			if (_bufferPosition >= BufferSize)
			{
				Flush();
			}

			_buffer[_bufferPosition++] = b;
		}

		public override void WriteBytes(byte[] b, int offset, int length)
		{
			// lazy create buffer.
			_buffer ??= new byte[BufferSize];

			int bytesLeft = BufferSize - _bufferPosition;

			// is there enough space in the buffer?
			if (bytesLeft >= length)
			{
				// add the data to the end of the buffer
				Buffer.BlockCopy(b, offset, _buffer, _bufferPosition, length);
				_bufferPosition += length;

				// if the buffer is full, flush it
				if (_bufferPosition == BufferSize)
				{
					Flush();
				}
			}
			else
			{
				// is the data larger than the buffer?
				if (length > BufferSize)
				{
					// flush whatever is already in the buffer
					if (_bufferPosition > 0)
					{
						Flush();
					}
					// and write data at once
					// crc.Update(b, offset, length);
					FlushBuffer(b, offset, length);
				}
				else
				{
					// we fill/flush the buffer (until the input is written)
					int pos = 0; // position in the input data
					int pieceLength;
					while (pos < length)
					{
						pieceLength = (length - pos < bytesLeft) ? length - pos : bytesLeft;
						Buffer.BlockCopy(b, pos + offset, _buffer, _bufferPosition, pieceLength);
						pos += pieceLength;
						_bufferPosition += pieceLength;
						// if the buffer is full, flush it
						bytesLeft = BufferSize - _bufferPosition;
						if (bytesLeft == 0)
						{
							Flush();
							bytesLeft = BufferSize;
						}
					}
				}
			}
		}

		protected virtual void FlushBuffer(byte[] b, int offset, int length)
		{
			// reset the buffer.
			_bufferPosition = 0;

			// is the length greater than max allowed size?
			if (length > MaxLength)
			{
				// flush the buffer in batches of max size.
				do
				{
					// ensure the page blob is large enough.
					EnsureSizeOrResize(MaxLength);

					using MemoryStream stream = new MemoryStream(b, offset, MaxLength);
					_pageBlobClient.UploadPages(stream, _currPageOffset); // write to the page blob.
					_currPagePosition += MaxLength; // reflects the size of the page blob after write.
					SetCurrentPageOffset();
					offset += MaxLength; // advance the offset by max length.
					length -= MaxLength; // reduce length by max length.

				} while (length > MaxLength);
			}

			// would all the "length" bytes fit exactly into pages of 512 bytes each?
			int excessLength = length % 512;
			if (length <= 512 || excessLength == 0)
			{
				// ensure the page is large enough.
				EnsureSizeOrResize(length);

				// upload the pages.
				byte[] scratch = new byte[512];
				Buffer.BlockCopy(b, offset, scratch, 0, length);
				using MemoryStream stream = new MemoryStream(scratch, 0, 512);
				PageInfo pageInfo = _pageBlobClient.UploadPages(stream, _currPageOffset);
				_currPagePosition += length;
				SetCurrentPageOffset();
			}
			else
			{
				// ensure the page is large enough.
				EnsureSizeOrResize(length - excessLength + 512);

				using (MemoryStream stream = new MemoryStream(b, offset, length - excessLength))
				{
					PageInfo pageInfo = _pageBlobClient.UploadPages(stream, _currPageOffset);
					_currPagePosition += length - excessLength;
					SetCurrentPageOffset();
				}

				// don't yet write the excess bytes. 
				// add them back to the buffer.
				Buffer.BlockCopy(b, length - excessLength, _buffer, _bufferPosition, excessLength);
				_bufferPosition += excessLength;

				// TODO: test this path more.
				// mostly will fail, and requires to flush.

				// ensure the page is large enough.
				//EnsureSizeOrResize(excessLength);

				//using (MemoryStream stream = new MemoryStream(b, length - excessLength, excessLength))
				//{
				//	PageInfo pageInfo = _pageBlobClient.UploadPages(stream, _currPageOffset);
				//	// we don't update the blob offset yet because we still have the same data in the buffer.
				//	// this is an optimization to avoid sparse pages.
				//	// the next time a flush operation is executed, we resume writing on the same page
				//	// and possibly would overwrite a few bytes (up to a max of 511 bytes).
				//	// on the other hand, if the flush buffer were to never be called again, we are still
				//	// safe since we have already committed these excess bytes to the blob.
				//}
			}
		}

		private void SetCurrentPageOffset()
		{
			_currPageOffset = _currPagePosition - (_currPagePosition % 512);
		}

		private long GetCurrentPageOffset()
		{
			return _currPagePosition - (_currPagePosition % 512);
		}

		private void EnsureSizeOrResize(int bytes)
		{
			// since lucene does not do random writes, we can safely assume
			// that there have been no bytes written beyond the last recorded
			// blob position when the blob has been already sized to be large
			// enough to accomodate the minSize bytes specified.
			// this would only be from a previous resize operation having 
			// grown the file to a large enough size and not all bytes being 
			// used up thereafter.

			if (_lastPageOffset > _currPagePosition + bytes)
			{
				// no need to resize.
				return;
			}

			// calculate number of new pages to add w/ a buffer of 512KB.
			long newLastPageOffset = GetCurrentPageOffset() + ((bytes / PageSize) * PageSize) + 512;

			// resize blob.
			_ = _pageBlobClient.Resize(newLastPageOffset);

			// save actual bytes required.
			_metadata["actuallength"] = bytes.ToString();
			_pageBlobClient.SetMetadata(_metadata);

			// record the new offset for the last page.
			_lastPageOffset = newLastPageOffset;
		}

		public override void Flush()
		{
			FlushBuffer(_buffer, 0, _bufferPosition);
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				// write any pending bytes.
				Flush();
			}
		}
	}
}
