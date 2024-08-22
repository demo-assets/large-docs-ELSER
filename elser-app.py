from flask import Flask, request, render_template_string
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.exceptions import RequestError
from collections import Counter

app = Flask(__name__)

# Elasticsearch cloud configuration
CLOUD_ID = "Gen-AI:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbzo0NDMkNmIyMTk4MzMzYzQzNDA4OTk0NThlOGYyNWViYmQ0ZTckOTljY2NmNmNkNGMwNDEyM2IwMTM1MTkyZWIyYTUzYWI="  # Replace with your Elastic Cloud ID
CLOUD_USERNAME = "elastic"  # Replace with your Elastic username
CLOUD_PASSWORD = "hr8qyxemxwqGWWXHDX7PGt54"  # Replace with your Elastic password

# Initialize Elasticsearch client for Elastic Cloud
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(CLOUD_USERNAME, CLOUD_PASSWORD)
)

def query_chunks(index_name, query_text, top_n=5):
    """Query the index using ELSER and return matching chunks with their parent document IDs."""
    try:
        response = es.search(index=index_name, body={
            "query": {
                "text_expansion": {
                    "ml.tokens": {
                        "model_text": query_text,
                        "model_id": ".elser_model_2_linux-x86_64"
                    }
                }
            },
            "size": top_n * 2,  # Fetch more to ensure we get enough parent documents
            "_source": ["parent_id", "pdf_path", "content"]
        })
        return response
    except (NotFoundError, RequestError) as e:
        print(f"Error querying Elasticsearch: {e}")
        return None

def get_top_relevant_documents(index_name, query_text, top_n=5):
    """Get the top N most relevant parent documents for a given query text."""
    response = query_chunks(index_name, query_text, top_n=top_n)
    if not response:
        return None

    parent_ids = [hit["_source"]["parent_id"] for hit in response["hits"]["hits"] if "_source" in hit and "parent_id" in hit["_source"]]
    if not parent_ids:
        print("No matching chunks found")
        return None

    # Aggregate results to find the most relevant parent documents
    parent_id_counts = Counter(parent_ids)
    top_parent_ids = [parent_id for parent_id, count in parent_id_counts.most_common(top_n)]

    # Retrieve the top relevant parent documents
    relevant_documents = []
    for parent_id in top_parent_ids:
        try:
            parent_document = es.get(index=index_name, id=parent_id)
            doc = parent_document["_source"]
            relevant_documents.append({
                "document_name": parent_id,
                "link": doc.get("pdf_path"),
                "document": {k: v for k, v in doc.items() if k in ["content", "pdf_path"]}
            })
        except (NotFoundError, RequestError) as e:
            print(f"Error retrieving parent document {parent_id}: {e}")

    return relevant_documents

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        index_name = request.form['index_name']
        query_text = request.form['query_text']
        top_n = int(request.form['top_n'])
        documents = get_top_relevant_documents(index_name, query_text, top_n)
        return render_template_string(template, documents=documents, query_text=query_text, index_name=index_name, top_n=top_n)
    return render_template_string(template, documents=None)

template = '''
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>BAM ELSER Search</title>
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body>
    <div class="container">
      <h1 class="mt-5">BAM ELSER Search</h1>
      <form method="post" class="mt-4">
        <div class="form-group">
          <label for="index_name">Index Name</label>
          <input type="text" class="form-control" id="index_name" name="index_name" required>
        </div>
        <div class="form-group">
          <label for="query_text">Query Text</label>
          <input type="text" class="form-control" id="query_text" name="query_text" required>
        </div>
        <div class="form-group">
          <label for="top_n">Number of Results</label>
          <input type="number" class="form-control" id="top_n" name="top_n" required>
        </div>
        <button type="submit" class="btn btn-primary">Search</button>
      </form>
      {% if documents %}
      <h2 class="mt-5">Results for "{{ query_text }}"</h2>
      <div id="accordion">
        {% for doc in documents %}
        <div class="card">
          <div class="card-header" id="heading{{ loop.index }}">
            <h5 class="mb-0">
              <button class="btn btn-link" data-toggle="collapse" data-target="#collapse{{ loop.index }}" aria-expanded="true" aria-controls="collapse{{ loop.index }}">
                {{ doc.document_name }} ({{ doc.link }})
              </button>
            </h5>
          </div>
          <div id="collapse{{ loop.index }}" class="collapse" aria-labelledby="heading{{ loop.index }}" data-parent="#accordion">
            <div class="card-body">
              <pre>{{ doc.document.content }}</pre>
            </div>
          </div>
        </div>
        {% endfor %}
      </div>
      {% endif %}
    </div>
    <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js"></script>
  </body>
</html>
'''

if __name__ == "__main__":
    app.run(debug=True)
