# Industry Documents Wrapper

This is a simple Python Wrapper for the UCSF [Industry Documents Library](https://www.industrydocuments.ucsf.edu/) API. Documentation about the API can be found [here](https://www.industrydocuments.ucsf.edu/wp-content/uploads/2020/08/IndustryDocumentsDataAPI_v7.pdf). Please use the API documentation to learn about best practices to construct search queries. 

It offers basic functionality to perform queries on the API to retrieve metadata of the documents in the library. 

You will want to install the package using `pip`:

`pip install industryDocumentsWrapper`


The the package has one class `IndustryDocsSearch` with two main methods of are:
* `IndustryDocsSearch.query()`: performs the query on the API 
* `IndustryDocsSearch.save()`: saves query results as a JSON or Parquet file.

Basic usage looks like: 

```
from industryDocumentsWrapper import IndustryDocsSearch 

wrapper = IndustryDocsSearch()
wrapper.query(q="industry:tobacco AND case:'State of North Carolina' AND collection:'JUUL labs Collection', n=100")
wrapper.save('query_results.json', format='json')
```

Alternatively, to avoid constructing the whole query, you can pass parts of the query as arguments: 

```
from industryDocumentsWrapper import IndustryDocsSearch 

wrapper = IndustryDocsSearch()
wrapper.query(industry='tobacco', case='State of North Carolina', collection='JUUl labs collection', n=100)
wrapper.save('query_results.json', format='json')
```

Currently there is support for the following parameters: 
* `q`: complete query string
* `case`: Case pertaining to documents 
* `collection`: Collection of which documents are part
* `type`: Type of documents 
* `industry`: Industry of which documents are part
* `brand`: Brand to which documents pertain
* `availability`: Availability of documents
* `date`: Date documents were created
* `id`: ID of particular document
* `author`: Creator of document(s)
* `source`: Source of document(s)
* `bates`: Bates code for document
* `originalformat`: Original format that documents were created
* `n`: Number of documents you want to retrieve. Pass `-1` to retrieve all documents returned by the query. Defaults to `1000`.

**NOTE:** The query method will use the `q` parameter instead of the others (excluding `n`) if it is passed, please use the `q` parameter or pass the values with the individual parameters (`case`, `collection`, etc.).

**For guidance on the proper way to pass values in the query, please refer to the [API documentation](https://www.industrydocuments.ucsf.edu/wp-content/uploads/2020/08/IndustryDocumentsDataAPI_v7.pdf).**

Please reach out to [Rolando Rodriguez](mailto:rolando@ad.unc.edu) with any questions, concerns, or issues.
