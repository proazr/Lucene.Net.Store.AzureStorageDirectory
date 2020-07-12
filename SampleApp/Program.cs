using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Codecs.SimpleText;
using Lucene.Net.Documents;
using Lucene.Net.Documents.Extensions;
using Lucene.Net.Index;
using Lucene.Net.Store.AzureStorageDirectory;
using Lucene.Net.Util;
using Microsoft.Extensions.Configuration;

namespace SampleApp
{
	class Program
	{
		const LuceneVersion LuceneVersion = Lucene.Net.Util.LuceneVersion.LUCENE_48;
		static string s_connectionString;
		static string s_containerNameStandard;
		static string s_containerNameSimple;

		static void Main(string[] args)
		{
			IConfiguration config = new ConfigurationBuilder()
				.AddUserSecrets("4543cd6d-e494-4246-99c6-6b5fcab08d20")
				.Build();

			s_connectionString = config.GetSection("connectionString").Value;
			s_containerNameStandard = config.GetSection("containerNameStandard").Value;
			s_containerNameSimple = config.GetSection("containerNameSimple").Value;

			BuildIndexSimple();
			// ReadIndex();
			Console.ReadLine();
		}

		static void BuildIndexStandard()
		{
			var info = new StringBuilder();
			var infoStream = new StringWriter(info);

			var directory = new AzureStorageDirectory(s_connectionString, s_containerNameStandard, "luctest");
			IndexWriterConfig config = new IndexWriterConfig(LuceneVersion, new StandardAnalyzer(LuceneVersion))
			{
				OpenMode = OpenMode.CREATE_OR_APPEND
			}.SetInfoStream(infoStream);

			var indexWriter = new IndexWriter(directory, config);

			foreach (var doc in GetDocs())
			{
				indexWriter.AddDocument(doc);
			}

			indexWriter.Commit();
			indexWriter.Dispose();

			Console.WriteLine(info.ToString());
		}

		static void BuildIndexSimple()
		{
			var info = new StringBuilder();
			var infoStream = new StringWriter(info);

			var directory = new AzureStorageDirectory(s_connectionString, s_containerNameSimple, "luctest");
			IndexWriterConfig config = new IndexWriterConfig(LuceneVersion, new StandardAnalyzer(LuceneVersion))
			{
				OpenMode = OpenMode.CREATE_OR_APPEND,
				Codec = new SimpleTextCodec()
			}.SetInfoStream(infoStream);

			var indexWriter = new IndexWriter(directory, config);

			foreach (var doc in GetDocs())
			{
				indexWriter.AddDocument(doc);
			}

			indexWriter.Commit();
			indexWriter.Dispose();

			Console.WriteLine(info.ToString());
		}


		static void ReadIndex()
		{
			var directory = new AzureStorageDirectory(s_connectionString, s_containerNameStandard, "luctest");

			var directoryReader = DirectoryReader.Open(directory);
			var numDocs = directoryReader.NumDocs;
			var maxDocs = directoryReader.MaxDoc;
			var version = directoryReader.Version;
		}

		static IEnumerable<Document> GetDocs()
		{
			var doc1 = new Document();
			doc1.AddTextField("Name", "Tyson", Field.Store.YES);
			doc1.AddTextField("Skill", "Boxer", Field.Store.YES);

			yield return doc1;

			var doc2 = new Document();
			doc2.AddTextField("Name", "Phelps", Field.Store.YES);
			doc2.AddTextField("Skill", "Swimmer", Field.Store.YES);

			yield return doc2;
		}
	}
}
