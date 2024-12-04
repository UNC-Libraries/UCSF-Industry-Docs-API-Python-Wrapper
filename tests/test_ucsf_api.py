import pytest
from unittest import mock
from industryDocumentsWrapper.ucsf_api import IndustryDocsSearch

# Mock the requests.get() response
@pytest.fixture
def mock_json_response():
    yield {
        "responseHeader": {
            "status": 0,
            "QTime": 500,
            "params": {
                "q": "industry:tobacco",
                "cursorMark": "*",
                "collection": "\"JUUL labs Collection\"",
                "sort": "id desc",
                "type": "email",
                "wt": "json",
                "case": "'State of North Carolina'"
            }
        },
        "response": {
            "numFound": 200,
            "start": 0,
            "numFoundExact": True,
            "docs": [
                {
                    "id": f"aazz{i:03}",
                    "collection": ["JUUL Labs Collection"],
                    "titles": f"Document {i}",
                    "bates": f"CLI0101{i:03}"
                } for i in range(100)
            ]
        },
        "nextCursorMark": "ABC123"
    }

@pytest.fixture
def mock_results(): 
    yield [
                {
                    "id": f"aazz{i:03}",
                    "collection": ["JUUL Labs Collection"],
                    "titles": f"Document {i}",
                    "bates": f"CLI0101{i:03}"
                } for i in range(100)
            ]

@pytest.fixture(scope='module')
def indDocSearch():
    yield IndustryDocsSearch()
    
@pytest.fixture(autouse=True)
def reset_results(indDocSearch):
    indDocSearch.results = []
    
# Tests the IndustryDocsSearch methods
    
def test_create_query_with_q(indDocSearch):
    assert indDocSearch._create_query(q='collection:test AND industry:tobacco', wt='json', cursorMark='*', sort='id%20asc') == 'https://metadata.idl.ucsf.edu/solr/ltdl3/query?q=(collection:test AND industry:tobacco)&wt=json&cursorMark=*&sort=id%20asc'
    
def test_create_query_without_q(indDocSearch):
    assert indDocSearch._create_query(q=False, collection='test', industry='tobacco', wt='json', cursorMark='*', sort='id%20asc') == 'https://metadata.idl.ucsf.edu/solr/ltdl3/query?q=(collection:test AND industry:tobacco)&wt=json&cursorMark=*&sort=id%20asc'
    
def test_update_cursormark(indDocSearch):
    query = 'https://metadata.idl.ucsf.edu/solr/ltdl3/query?q=(collection:test)&wt=json&cursorMark=*&sort=id%20asc'
    assert indDocSearch._update_cursormark(query, '123AB') == 'https://metadata.idl.ucsf.edu/solr/ltdl3/query?q=(collection:test)&wt=json&cursorMark=123AB&sort=id%20asc'

def test_loop_results_50(indDocSearch):
    query = 'https://metadata.idl.ucsf.edu/solr/ltdl3/query?q=(industry:tobacco AND case:"State of North Carolina" AND collection:"JUUL labs Collection" AND type:email)&wt=json&cursorMark=*&sort=id%20asc'
    indDocSearch._loop_results(query, 50)
    assert len(indDocSearch.results) == 50
    assert len(set([x['id'] for x in indDocSearch.results])) == 50
    assert indDocSearch.results[0]['id'] == 'ffbb0284'
    
def test_loop_results_150(indDocSearch):
    query = 'https://metadata.idl.ucsf.edu/solr/ltdl3/query?q=(industry:tobacco AND case:"State of North Carolina" AND collection:"JUUL labs Collection" AND type:email)&wt=json&cursorMark=*&sort=id%20asc'
    indDocSearch._loop_results(query, 150)
    assert len(indDocSearch.results) == 150
    assert len(set([x['id'] for x in indDocSearch.results])) == 150
    assert indDocSearch.results[0]['id'] == 'ffbb0284'

def test_create_links(indDocSearch, mock_results):
    indDocSearch.results = mock_results
    industry = 'tobacco'
    indDocSearch._create_links(industry)
    assert indDocSearch.results[0]['url'] == 'https://www.industrydocuments.ucsf.edu/tobacco/docs/#id=aazz000'
    
def test_query_with_q_100(indDocSearch):
    indDocSearch.query(q='industry:tobacco AND case:"State of North Carolina" AND collection:"JUUL labs Collection" AND type:email', n=100)
    assert len(indDocSearch.results) == 100
    assert len(set([x['id'] for x in indDocSearch.results])) == 100
    assert indDocSearch.results[0]['id'] == 'ffbb0284'
    assert indDocSearch.results[0]['url'] == 'https://www.industrydocuments.ucsf.edu/tobacco/docs/#id=ffbb0284'
    
def test_query_with_q_500(indDocSearch):
    indDocSearch.query(q='industry:tobacco AND case:"State of North Carolina" AND collection:"JUUL labs Collection" AND type:email', n=500)
    assert len(indDocSearch.results) == 500
    assert len(set([x['id'] for x in indDocSearch.results])) == 500
    assert indDocSearch.results[0]['id'] == 'ffbb0284'
    assert indDocSearch.results[0]['url'] == 'https://www.industrydocuments.ucsf.edu/tobacco/docs/#id=ffbb0284'

def test_query_with_no_q_50(indDocSearch):
    indDocSearch.query(industry='tobacco', collection='JUUL labs Collection', case='State of North Carolina', doc_type='email', n=50)
    assert len(indDocSearch.results) == 50
    assert len(set([x['id'] for x in indDocSearch.results])) == 50
    
def test_query_with_no_q_1000(indDocSearch):
    indDocSearch.query(industry='tobacco', collection='JUUL labs Collection', case='State of North Carolina', doc_type='email', n=1000)
    assert len(indDocSearch.results) == 1000
    assert len(set([x['id'] for x in indDocSearch.results])) == 1000

def test_save_parquet(indDocSearch, mock_results, tmp_path):
    indDocSearch.results = mock_results
    
    d = tmp_path / 'test.parquet'
    
    indDocSearch.save(d, format='parquet')
    
    assert d.exists()
    assert d.stat().st_size > 0

# def test_save_csv(indDocSearch, mock_results, tmp_path):
#     indDocSearch.results = mock_results
#     d = tmp_path / 'test.csv'
#     indDocSearch.save(d, format='csv')
#     assert d.exists()
#     assert d.stat().st_size > 0

def test_save_json(indDocSearch, mock_results, tmp_path):
    indDocSearch.results = mock_results
    d = tmp_path / 'test.json'
    indDocSearch.save(d, format='json')
    assert d.exists()
    assert d.stat().st_size > 0