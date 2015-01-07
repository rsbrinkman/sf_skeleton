from flask import Flask, render_template
from flask import request
from sqlalchemy import *
import query_templates
import db
import requests
from requests.auth import HTTPBasicAuth
import json

app = Flask(__name__)


@app.route("/")
def main():
   context = {}
   # Modified Intervals
   context['modified_intervals'] = db.get_modified_intervals()
   # Rep Deals
   context['rep_deals'] = db.get_rep_deals()
   # Pipeline Movements
   context['pipeline_movements'] = db.get_pipeline_movements()

   return render_template('index.html', context=context) 

   
@app.route("/details")
def details():
   context = {}
   stage = request.args.get('stage')
   context['stage'] = stage
   name = request.args.get('name')
   context['name'] = name
   se_name = request.args.get('se_name')
   context['se_name'] = se_name
   if se_name:
      context['stage_details'] = db.get_se_opps(stage, se_name)
   else:
      context['stage_details'] = db.get_opp_details(stage, name)
   
   return render_template('details.html', context=context)

@app.route("/se_team")
def se_details():
   context = {}
   context['se_summary'] = db.get_se_details()
   context['se_comp_product_completion'] = db.get_se_competitor_product()

   return render_template('se_details.html', context=context)

@app.route("/cron")
def cron():
   
   kaiser_url = "http://kaiser.kontagent.com/dashboard/datamine2/query/"
   kaiser_query = """SELECT get_json_object(evt.json_data, '$.lat') as Lat, get_json_object(evt.json_data, '$.long') as Lon, n, s, v, l, st1, st2, st3 FROM ebwiosqa_evt evt WHERE month = 201306 AND get_json_object(evt.json_data, '$.lat') != 'NULL'"""
   url = "http://kontagent.kontagent.com/dashboard/datamine2/query/"
   headers = {'content-type': 'application/json'}
   payload = {'query': 'select * from dashboard0_evt LIMIT 10'}
   kaiser_payload = {'query': "SELECT get_json_object(evt.json_data, '$.lat') as Lat, get_json_object(evt.json_data, '$.long') as Lon, n, s, v, l, st1, st2, st3 FROM ebwiosqa_evt evt WHERE month = 201306 AND get_json_object(evt.json_data, '$.lat') != 'NULL'"}
            
   query = db.build_payload(payload)
   print query
   r = requests.post(url, data=query, headers=headers, auth=HTTPBasicAuth('scott.brinkman@kontagent.com', 'Fr3nchdip'))
   payload = db.build_payload({'limit':None, 'offset':None, 'download':None}) 
   results = requests.post('%s%s/results/' % (url, json.loads(r.content)['id']), params=payload, headers=headers, auth=('datamine@kontagent.com','kont@gent'))

   table = results.content.split('\r\n') 
   context = table
   return render_template('heatmap.html', context=context)



if __name__ == "__main__":
    app.run(host='10.50.9.63')

