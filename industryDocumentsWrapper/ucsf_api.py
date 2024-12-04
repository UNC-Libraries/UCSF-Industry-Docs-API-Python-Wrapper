from dataclasses import dataclass
import re
import requests
import polars as pl
    

@dataclass
class IndustryDocsSearch:
    """
    UCSF Industry Documents Library Solr API Wrapper Class.

    API Documentation found here: https://www.industrydocuments.ucsf.edu/wp-content/uploads/2020/08/IndustryDocumentsDataAPI_v7.pdf
    """
    base_url = "https://metadata.idl.ucsf.edu/solr/ltdl3/"
    results = []
    
    def _create_query(self, **kwargs) -> str:
        """Constructs parametrized query"""
        if kwargs['q']:
            query = f"{self.base_url}query?q=({kwargs['q']})&wt={kwargs['wt']}&cursorMark={kwargs['cursorMark']}&sort={kwargs['sort']}"
        else:
            query = f"{self.base_url}query?q=("+' AND '.join([f'{k}:{v}' for k, v in kwargs.items() if v and k != 'wt' and k != 'cursorMark' and k != 'sort' and k != 'n'])+f")&wt={kwargs['wt']}&cursorMark={kwargs['cursorMark']}&sort={kwargs['sort']}"
       
        return query
    
    def _update_cursormark(self, query:str, cursor_mark: str) -> str:
        """Updates cursor mark in query string"""
        return re.sub(r'(?<=cursorMark=)[A-Za-z0-9*=]+(?=&)', cursor_mark, query)
    
    def _loop_results(self, query:str, n:int) -> None:
        """Iteratively retrieves documents with cursor_mark for Solr deep paging"""
        next_cursor = None 
        current_cursor = '*' # initial cursor mark
        
        if n == -1:
            n = float('inf')
            
        while (next_cursor != current_cursor) and (len(self.results) < n):

            if next_cursor:
                current_cursor = next_cursor
                query = self._update_cursormark(query, current_cursor)
            
            r = requests.get(query).json()
            
            if n < len(r['response']['docs']):
                self.results.extend(r['response']['docs'][:n])
            
            elif n < (len(self.results) + len(r['response']['docs'])):
                self.results.extend(r['response']['docs'][:n-len(self.results)])
                
            else:
                self.results.extend(r['response']['docs'])
            
            next_cursor = r['nextCursorMark']
                            
            print(f"{len(self.results)}/{n} documents collected")
                
        return
    
    def _create_links(self, industry) -> None:
        """Adds links to documents"""
        for doc in self.results:
            doc['url'] = f"https://www.industrydocuments.ucsf.edu/{industry}/docs/#id={doc['id']}"
    
    def query(self, 
        q:str = False,
        case:str = False,
        collection:str = False,
        doc_type:str = False,
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
                             type=doc_type, 
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
        if not filename.lower().endswith('.parquet'):
            raise Exception("Only parquet format supported currently.")
        self.results = pl.read_parquet(filename)


    def save(self, filename: str, format: str) -> None:
        """Writes previously queried results into Polars dataframe then saves in specified format"""
        df = pl.DataFrame(self.results, nan_to_null=True)
        match format:
            case 'parquet':
                df.write_parquet(filename)
            # case 'csv':
            #     df = df.with_columns(pl.col(pl.List, pl.Struct, pl.Array).list.join(","))
            #     df.write_csv(filename)
            case 'json':
                df.write_json(filename)
            case _:
                raise Exception("Only parquet and json formats supported currently.")
        