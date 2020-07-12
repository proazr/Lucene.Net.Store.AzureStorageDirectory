using System.Collections.Generic;
using System.IO;

namespace Lucene.Net.Store.AzureStorageDirectory
{
	public class AzureStorageLockFactory : LockFactory
	{
		private readonly AzureStorageDirectory _directory;
		private readonly IDictionary<string, Lock> _locks;

		public AzureStorageLockFactory(AzureStorageDirectory directory)
		{
			_directory = directory;
			_locks = new Dictionary<string, Lock>();
		}

		public override void ClearLock(string lockName)
		{
			var lockPath = GetLockPath(lockName);

			lock (_locks)
			{
				if (_locks.TryGetValue(lockPath, out Lock @lock))
				{
					_locks.Remove(lockPath);
					@lock.Dispose();
				}
			}
		}

		public override Lock MakeLock(string lockName)
		{
			Lock @lock = null;
			var lockPath = GetLockPath(lockName);

			lock (_locks)
			{
				if (!_locks.TryGetValue(lockPath, out @lock))
				{
					@lock = new AzureStorageLock(_directory, lockPath);
					_locks.Add(lockPath, @lock);
				}
			}

			return @lock;
		}

		private string GetLockPath(string lockName)
		{
			return Path.Combine(_directory.IndexFolder, (LockPrefix is null ? lockName : $"{LockPrefix}-{lockName}"));
		}
	}
}