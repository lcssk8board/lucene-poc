namespace Lucene.POC
{
    using Engine;
    using Models;
    using System;

    public class Program
    {
        static void Main(string[] args)
        {
            const string LUCENE_INDEX = "lucene_index";
            using (var lucene = new LuceneHandler(LUCENE_INDEX, Net.Util.Version.LUCENE_30, true))
            {
                lucene.AddDocument(new SampleData {
                    Id = 1,
                    Name = "teste",
                    Description = "teste2"
                }, new SampleData
                {
                    Id = 2,
                    Name = "teste",
                    Description = "teste2"
                }, new SampleData
                {
                    Id = 3,
                    Name = "teste",
                    Description = "teste2"
                }, new SampleData
                {
                    Id = 4,
                    Name = "teste",
                    Description = "teste2"
                });

                lucene.UpdateDocument(new SampleData
                {
                    Id = 1,
                    Name = "teste2",
                    Description = "teste2"
                });

                lucene.AddDocument(new SampleData
                {
                    Id = 2,
                    Name = "teste",
                    Description = "teste2"
                });

                lucene.DeleteDocument(new SampleData
                {
                    Id = 2,
                    Name = "teste",
                    Description = "teste2"
                });

                var allDocuments = lucene.FindDocument<SampleData>("*:*", 1000);
                foreach (var document in allDocuments)
                    Console.WriteLine(document.ToString());
                
            }

            Console.Read();
        }
    }
}

namespace Lucene.POC.Engine
{
    using Models;
    using Net.Analysis.Standard;
    using Net.Index;
    using Net.Store;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using SystemDirectory = System.IO.Directory;
    using SystemPath = System.IO.Path;
    using LuceneVersion = Net.Util.Version;
    using Net.Documents;
    using Net.Search;
    using Net.QueryParsers;
    using Net.Analysis;
    using System.Linq;
    using Attributes;

    public interface ILuceneHandler
    {
        void AddDocument<TModel>(params TModel[] documents) where TModel : class, new();
        void UpdateDocument<TModel>(params TModel[] documents) where TModel : class, new();
        void DeleteDocument<TModel>(params TModel[] documents) where TModel : class, new();
        IList<TModel> FindDocument<TModel>(string query, int maxHits) where TModel : class, new();
    }

    public sealed class LuceneHandler : ILuceneHandler, IDisposable
    {
        private static string APP_PATH = AppDomain.CurrentDomain.BaseDirectory;

        private readonly LuceneVersion _luceneVersion;
        private readonly string _luceneIndexPath;
        private readonly FSDirectory _luceneDir;

        public LuceneHandler(string luceneIndexPath, LuceneVersion luceneVersion, bool createFolderIfNotFound)
        {
            if (string.IsNullOrWhiteSpace(_luceneIndexPath = luceneIndexPath))
                throw new ArgumentNullException(nameof(luceneIndexPath));

            if (!SystemDirectory.Exists(_luceneIndexPath = SystemPath.Combine(APP_PATH, _luceneIndexPath)))
            {
                if (createFolderIfNotFound)
                    SystemDirectory.CreateDirectory(_luceneIndexPath);
                else
                    throw new ArgumentException($"Path {_luceneIndexPath} not found");
            }

            var oldLockFilePath = SystemPath.Combine(_luceneIndexPath, "write.lock");

            _luceneVersion = luceneVersion;
            _luceneDir = FSDirectory.Open(new DirectoryInfo(_luceneIndexPath));

            if (IndexWriter.IsLocked(_luceneDir))
                IndexWriter.Unlock(_luceneDir);

            if (File.Exists(oldLockFilePath))
                File.Delete(oldLockFilePath);
        }

        public void AddDocument<TModel>(params TModel[] documents)
            where TModel : class, new()
        {
            IndexWriteContext(writer => {
                foreach (var model in documents)
                {
                    var document = new Document();
                    var entity = MapToFields(model);

                    foreach (var fields in entity)
                        document.Add(new Field(fields.Field, fields.Value, fields.Store, fields.Index));

                    writer.AddDocument(document);
                    writer.Optimize();
                }
            });
        }

        public void DeleteDocument<TModel>(params TModel[] documents)
            where TModel : class, new()
        {
            IndexWriteContext(writer => {
                foreach (var model in documents)
                {
                    var fields = MapToFields(model);
                    var ids = fields.Where(f => f.IsKey).ToList();

                    if (ids != null && ids.Count > 0)
                    {
                        var query = new BooleanQuery();

                        foreach (var id in ids)
                            query.Add(new BooleanClause(new TermQuery(new Term(id.Field, id.Value)), Occur.MUST));

                        writer.DeleteDocuments(query);
                    }
                }
            });
        }

        public IList<TModel> FindDocument<TModel>(string query, int maxHits)
            where TModel: class, new()
        {
            var documents = new List<TModel>(maxHits);

            using (var analyzer = new StandardAnalyzer(_luceneVersion))
            using (var searcher = new IndexSearcher(_luceneDir, true))
            {
                var parsedQuery = ParseQuery(query, analyzer);
                var hits = searcher.Search(parsedQuery, maxHits);

                var luceneDocuments = new Document[hits.ScoreDocs.Length];

                for (int i = 0; i < hits.ScoreDocs.Length; i++) {
                    var hit = hits.ScoreDocs[i];
                    luceneDocuments[i] = searcher.Doc(hit.Doc);
                }

                var modelMapped = MapToModel<TModel>(luceneDocuments);
                documents.AddRange(modelMapped);
            }

            return documents;
        }

        public void UpdateDocument<TModel>(params TModel[] documents)
            where TModel : class, new()
        {
            IndexWriteContext(writer => {
                foreach (var model in documents)
                {
                    var document = new Document();
                    var entity = MapToFields(model);
                    var keys = entity.Where(e => e.IsKey).ToList();

                    if (keys == null || keys.Count == 0)
                        throw new ArgumentException("Document without a primary key");

                    var query = new BooleanQuery();

                    foreach (var key in keys)
                        query.Add(new BooleanClause(new TermQuery(new Term(key.Field, key.Value)), Occur.MUST));

                    foreach (var fields in entity)
                        document.Add(new Field(fields.Field, fields.Value, fields.Store, fields.Index));

                    writer.DeleteDocuments(query);
                    writer.AddDocument(document);
                    writer.Optimize();
                }
            });
        }

        public void Dispose()
        {
            _luceneDir.Dispose();
        }

        private void IndexWriteContext(Action<IndexWriter> writingMethod)
        {
            using (var analyzer = new StandardAnalyzer(_luceneVersion))
            using (var writer = new IndexWriter(_luceneDir, analyzer, IndexWriter.MaxFieldLength.UNLIMITED))
            {
                try
                {
                    writingMethod(writer);
                    writer.Commit();
                }
                catch (Exception ex)
                {
                    writer.Rollback();
                    throw ex;
                }
            }
        }

        private IList<LuceneFieldModel> MapToFields<TModel>(TModel model)
            where TModel : class, new()
        {
            var properties = model.GetType().GetProperties();
            var luceneFields = new List<LuceneFieldModel>(properties.Length);

            foreach (var property in properties)
            {
                luceneFields.Add(new LuceneFieldModel
                {
                    Field = property.Name,
                    Value = property.GetValue(model).ToString(),
                    Index = Field.Index.NOT_ANALYZED,
                    Store = Field.Store.YES,
                    IsKey = property.GetCustomAttributes(typeof(LuceneKey), false).Length > 0
                });
            }

            return luceneFields;
        }

        private IList<TModel> MapToModel<TModel>(Document[] documents)
            where TModel: class, new()
        {
            if (documents == null)
                throw new ArgumentNullException(nameof(documents));

            var typedDocuments = new List<TModel>(documents.Length);

            foreach (var document in documents)
            {
                var model = new TModel();
                var allProperties = model.GetType().GetProperties();

                foreach (var property in allProperties)
                {
                    var propertyValue = document.Get(property.Name);

                    if (propertyValue != null)
                        property.SetValue(model, Convert.ChangeType(propertyValue, property.PropertyType));
                }

                typedDocuments.Add(model);
            }

            return typedDocuments;
        }

        private Query ParseQuery(string query, Analyzer analyzer)
        {
            var luceneQuery = default(Query);
            var parser = new QueryParser(_luceneVersion, query, analyzer);

            try
            {
                luceneQuery = parser.Parse(query.Trim());
            }
            catch (ParseException)
            {
                luceneQuery = parser.Parse(QueryParser.Escape(query.Trim()));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception ocurred: {ex.ToString()}");
            }

            return luceneQuery;
        }
    }
}

namespace Lucene.POC.Engine.Models
{
    using static Net.Documents.Field;
    public class LuceneFieldModel
    {
        public string Field { get; set; }
        public string Value { get; set; }
        public Store Store { get; set; }
        public Index Index { get; set; }
        public bool IsKey { get; set; }
    }
}

namespace Lucene.POC.Models
{
    using Attributes;
    public class SampleData
    {
        [LuceneKey]
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }

        public override string ToString()
        {
            return $"Id: {Id}, Name: {Name}, Description: {Description}";
        }
    }
}

namespace Lucene.POC.Attributes
{
    using System;
    public class LuceneKey : Attribute
    {
        public LuceneKey()
        {

        }
    }
}
