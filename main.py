
from sqlalchemy import create_engine, DateTime, Table, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
import pandas as pd
import sqlalchemy
import articles as ar


engine = create_engine("postgresql://xui:mmceez@localhost:5433/tumba", echo = True)
Session = sessionmaker(bind = engine)
session = Session()
engine.connect()
BaseModel = declarative_base()
meta = MetaData()

class Article(BaseModel):
	__tablename__ = 'test_articles_1'

	id = Column(Integer, primary_key = True, autoincrement = True, nullable = False)
	pmid = Column(Integer, nullable = False)
	publ_date = Column(DateTime)
	authors_count = Column(Integer)
	pi_name = Column(String)
	full_affil = Column(postgresql.ARRAY(String))
	pi_affil = Column(postgresql.ARRAY(String))
	pi_publ_count = Column(Integer)
	journal_publ_count = Column(Integer)
	cit_count = Column(Integer)
	journal = Column(String)
	summary = Column(postgresql.JSON)
	#scopus_json = Column(postgresql.JSON)

	__columns__ = [
		'id',
		'pmid',
		'publ_date',
		'authors_count',
		'pi_name',
		'full_affil',
		'pi_affil',
		'pi_publ_count',
		'journal_publ_count',
		'cit_count',
		'journal',
		'summary',
		#'scopus_json',
	]


	def __init__(self, **kwargs):
		for key, value in kwargs.items():
			if key in Article.__columns__:
				setattr(self, key, value)
			else:
				raise Exception('Column {} is not exist in table'.format(key))



BaseModel.metadata.create_all(engine)


# def articles_to_db(keyword, max_results=50,retstart=0):

# 	# a = articles.try_or(pd.read_sql(sqlalchemy.text ("select * from test_articles;"), engine))
# 	# if a is None:


# 	Query = "select min(PMID) from test_articles"
# 	minpmid = pd.read_sql(sqlalchemy.text(Query), engine)


# 	pmids = ar.get_pmids(keyword,retstart=retstart,max_results=max_results)
# 	print(pmids)

# 	if str(minpmid.values[0][0]) in  pmids:
# 		retstart += pmids.index(str(minpmid.values[0][0])) + 1
# 		pmids = ar.get_pmids(keyword,retstart=retstart,max_results=max_results)
# 		print(pmids)
	    
# 	for count,pmid in enumerate(pmids):
		
# 	    args = ar.table_from_pmids(pmid)
# 	    a = Article(**args)
# 	    print(count,pmid)
# 	    session.add(a)
# 	    session.commit()

# 	return()   

def articles_to_db(keyword, max_results, max_sample_count, max_no_new_pmids_count):
    
    """finds articles PMIDs by keyword,  stores them into psql DB table.

    max_results - number of PMIDs retrieved from pubmed at once.  This is length of the subsequence  of all PMIDs found for given keyword and sorted by relevance. The subsiquence starts with random retsart.
    max_sample_count - max number of times the function retrieves a subsequence. Each time the subsequence is retrieved the function finds which PMIDs of it are not presented in the set of PMIDs in the DB table.
    max_no_new_pmids_count - maximum number of times the function finds no new PMIDs in a subsequence and retrieves the next one."""
    
    db_pmids = list(ar.get_table_col(colnames="pmid",table="test_articles")["pmid"])

    _, pubmed_res_count = ar.get_pmids(keyword=keyword, sortby="relevance", retstart=0, max_results=1)

    get_random = ar.randomizer(0,pubmed_res_count, max_results)
    

    count_no_new_pmids = 0
    iter_count = 0
    

    while (count_no_new_pmids < max_no_new_pmids_count) & (iter_count <= max_sample_count):
        iter_count+=1  
        retstart = get_random()
        
        print(f"iteration->{iter_count}<-iretation")
        print(f"retstart->{retstart}<-retstart") 

        if retstart is None:
            break

        pubmed_pmids, _ = ar.get_pmids(keyword=keyword, sortby="relevance", retstart=retstart, max_results=max_results)
        new_pmids = ar.get_list_diff(pubmed_pmids, db_pmids)


        if len(new_pmids) > 0:
            print(f"new pmids-> {new_pmids} <-new pmids")

            for pmid in new_pmids:

                db_pmids.append(pmid)
                args = ar.table_from_pmids(pmid)
                print("features are obtained")
                
                print(**args)

                a = Article(**args)
                session.add(a)
                session.commit()
                print(f"commited to table {pmid}")

        else:
            count_no_new_pmids += 1 
            print(f"no new pmids this time. count_no_new_pmids-> {count_no_new_pmids} <-count_no_new_pmids")
            
            
            
    return()

 

#articles_to_db(keyword="cancer", max_results=10, max_sample_count=5, max_no_new_pmids_count=10)	 
