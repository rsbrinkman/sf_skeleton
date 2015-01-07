from sqlalchemy import *
import query_templates
import datetime

YEAR_START = '2013-01-01'
QUARTER_START = '2013-05-01'
QUARTER_END = '2013-07-31'

def Query(query):
   engine = create_engine('mysql://root@localhost/Salesforce')
   conn = engine.connect()
   print query
   result = engine.execute(query)
   results = {}
   final_result = []
   for row in result:
      for k,v in row.items():
         results[k] = v
      final_result.append(results.copy())
   result.close()
   
   return final_result

def get_modified_intervals():
   
   results = Query(query_templates.last_modified_stage % ({'quarter_end': QUARTER_END, 'year_start': YEAR_START}))

   return results

def get_rep_deals():
   results = Query(query_templates.rep_deals % ({'quarter_end': QUARTER_END, 'year_start': YEAR_START}))

   return results

def get_pipeline_movements():
   
   results = Query(query_templates.pipeline_movements) 
  
   return results

def get_opp_details(stage=None, name=None):
   params = []
   if stage:
      params.append('AND StageName = \'%s\'' % stage)
   if name:
      params.append('AND UserName = \'%s\'' % name)
   
   results = Query(query_templates.opp_details % ({'params': ' '.join(params), 'quarter_end' : QUARTER_END, 'year_start': YEAR_START})) 

   return results

def get_se_details():
   
   results = Query(query_templates.se_details % ({'quarter_end': QUARTER_START, 'year_start': YEAR_START}))
   
   return results

def get_se_opps(stage, se_name):
   params = []
   if stage:
      params.append('AND StageName = \'%s\'' % stage)
   if se_name:
      params.append('AND opp.SeOwner = \'%s\'' % se_name)
   print query_templates.se_opp_details %  ({'params': ' '.join(params), 'quarter_end' : QUARTER_END, 'year_start': YEAR_START})
   results = Query(query_templates.se_opp_details %  ({'params': ' '.join(params), 'quarter_end' : QUARTER_END, 'year_start': YEAR_START}))

   return results

def get_se_competitor_product():

   results = Query(query_templates.se_product_competition % ({'quarter_end': QUARTER_END, 'year_start': YEAR_START}))
   print results
   return results

def build_payload(kwargs):
   payload = {}
   for k,v in kwargs.iteritems():
      if v is not None:
         payload[k] = unicode(v)
   return payload
