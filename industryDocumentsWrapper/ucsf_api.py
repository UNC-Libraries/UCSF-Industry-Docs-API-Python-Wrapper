from dataclasses import dataclass
import re
import time
import requests
import polars as pl
    

BATCH_TIMEOUT = 30  # seconds
RATE_LIMIT = 0.1    # seconds between requests

@dataclass
class IndustryDocsSearch:
    """
    UCSF Industry Documents Library Solr API Wrapper Class.

    API Documentation found here: https://www.industrydocuments.ucsf.edu/wp-content/uploads/2020/08/IndustryDocumentsDataAPI_v7.pdf
    """
    def __init__(self):
        self.__base_url = "https://metadata.idl.ucsf.edu/solr/ltdl3/"
        self.results = []
    
    def _create_query(self, **kwargs) -> str:
        """Constructs parametrized query"""
        if kwargs['q']:
            query = f"{self.__base_url}query?q=({kwargs['q']})&wt={kwargs['wt']}&cursorMark={kwargs['cursorMark']}&sort={kwargs['sort']}"
        else:
            query = f"{self.__base_url}query?q=("+' AND '.join([f'{k}:"{v}"' for k, v in kwargs.items() if v and k != 'wt' and k != 'cursorMark' and k != 'sort' and k != 'n'])+f")&wt={kwargs['wt']}&cursorMark={kwargs['cursorMark']}&sort={kwargs['sort']}"
        return query
    
    def _update_cursormark(self, query:str, cursor_mark: str) -> str:
        """Updates cursor mark in query string"""
        return re.sub(r'(?<=cursorMark=)[A-Za-z0-9*=]+(?=&)', cursor_mark, query)
    
    def _loop_results(self, query:str, n:int) -> None:
        """Iteratively retrieves documents with cursor_mark for Solr deep paging"""
        next_cursor = None 
        current_cursor = '*' # initial cursor mark
        
        # Get initial response to check total available documents
        initial_response = requests.get(query).json()
        total_available = initial_response['response']['numFound']
        
        if n > total_available:
            print(f"Warning: Only {total_available} documents available, which is less than the {n} requested")
            n = total_available
        
        if n == -1:
            n = total_available
            
        while (next_cursor != current_cursor) and (len(self.results) < n):
            if next_cursor:
                current_cursor = next_cursor
                query = self._update_cursormark(query, current_cursor)
            
            r = requests.get(query, timeout=BATCH_TIMEOUT).json()
            docs = r['response']['docs']
            
            if n < len(docs):
                self.results.extend(r['response']['docs'][:n])
            elif n < (len(self.results) + len(docs)):
                self.results.extend(r['response']['docs'][:n-len(self.results)])
            else:
                self.results.extend(docs)
            
            next_cursor = r['nextCursorMark']
                            
            print(f"{len(self.results)}/{n} documents collected")
            time.sleep(RATE_LIMIT)
                
        return
    
    def _create_links(self, industry) -> None:
        """Adds links to documents"""
        for doc in self.results:
            doc['url'] = f"https://www.industrydocuments.ucsf.edu/{industry}/docs/#id={doc['id']}"
    
    def query(self, 
        q:str = False,
        case:str = False,
        collection:str = False,
        type:str = False,
        industry:str = False,
        brand:str = False,
        availability:str = False,
        date:str = False,
        id:str = False,
        author:str = False,
        source:str = False,
        bates:str = False,
        box:str = False,
        originalformat:str = False,
        wt:str ='json',
        cursor_mark:str='*', 
        sort:str="id%20asc",
        n:int=1000) -> None:
        """Constructs original query string"""
        
        query = self._create_query(q=q, 
                             case=case, 
                             collection=collection, 
                             type=type, 
                             industry=industry, 
                             brand=brand, 
                             availability=availability, 
                             documentdate=date, 
                             id=id, 
                             author=author, 
                             source=source, 
                             batesexpanded=bates, 
                             box=box,
                             originalformat=originalformat, 
                             wt=wt, 
                             cursorMark=cursor_mark, 
                             sort=sort,
                             n=n)

        if re.search(r'(?<=industry:)\w+(?=\s)', query):
            industry = re.search(r'(?<=industry:)\w+(?=\s)', query).group()
            
        """Queries the UCSF Industry Documents Solr Library for documents"""
        self._loop_results(query, n)
        
        if industry:
            self._create_links(industry)

    # TODO: Determine whether we need to maintain this load method
    def load(self, filename: str) -> pl.DataFrame:
        """Reads results from a local CSV or JSON"""
        if filename.lower().endswith('.json'):
            self.results = pl.read_json(filename)
        elif filename.lower.endswith('.parquet'):
            self.results = pl.read_parquet(filename)
        elif filename.lower().endswith('.csv'):
            self.results = pl.read_csv(filename)


    def save(self, filename: str, format: str) -> None:
        """Writes previously queried results into Polars dataframe then saves in specified format"""
        df = pl.DataFrame(self.results, nan_to_null=True)
        match format:
            case 'parquet':
                df.write_parquet(filename)
            case 'csv':
                nested_cols = df.select([
                    pl.col(col) for col in df.columns 
                    if pl.DataFrame(df).schema[col] in [pl.List, pl.Struct, pl.Array]
                    ]).columns                
                if nested_cols:
                    df = df.with_columns([
                        pl.col(col).map_elements(lambda x: str(x) if x is not None else None, return_dtype=pl.Utf8) for col in nested_cols
                    ])
                df.write_csv(filename)
            case 'json':
                df.write_json(filename)
            case _:
                raise Exception("Only parquet and json formats supported currently.")
        