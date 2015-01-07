#!/usr/bin/python
import os
import urllib
import urllib2
import json
import pprint
import beatbox
import mysql.connector
import re
import datetime

USERNAME = ''
PASSWORD = ''
SECURITY_TOKEN = ''
SF_DATETIME = '%Y-%m-%dT%H:%M:%S.000Z'
SQL_DATETIME = '%Y-%m-%d %H:%M:%S'

def Query(query):
   cnx = mysql.connector.connect(user='root', password='',
                                 database='Salesforce')
   curA = cnx.cursor(buffered=True)
   curA.execute(query)
   cnx.commit()
   cnx.close()

def GetSFConn(username, password, security_token):
   password = str('%s%s' % (password, security_token))
   sf_client = beatbox.Client()
   sf_client.login(username, password)

   return sf_client

def InsertOpportunities(sf_client, sf):
   # Truncate Current Results
   Query("TRUNCATE Opportunities")
   #Query SF for results
   query = "select Id, Name, Amount, CloseDate, OwnerId, StageName, LastModifiedDate, Pre_Sales_Engineer__c, Product_Interest__c, Competitor__c from Opportunity"
   results = sf_client.query(query)
   accounts = {}
   for rec in results[sf.records:]:
      accounts['id'] = str(rec[2])
      accounts['name'] = re.escape(str(rec[3]))
      accounts['amount'] = str(rec[4])
      if not accounts['amount']:
         accounts['amount'] = 00.0
      accounts['close_date'] = str(rec[5])
      accounts['owner_id'] = str(rec[6])
      accounts['stage_name'] = str(rec[7])
      soql_date = datetime.datetime.strptime(str(rec[8]),SF_DATETIME)
      accounts['last_modified'] = soql_date.strftime(SQL_DATETIME)
      accounts['se_owner'] = str(rec[9])
      accounts['product_interest'] = str(rec[10])
      if accounts['product_interest'] == 'NULL' or accounts['product_interest'] == 'None':
         accounts['product_interest'] = ''
      accounts['competitors'] = str(rec[11])
      if accounts['competitors'] == 'NULL' or accounts['competitors'] == 'None':
         accounts['competitors'] = ''

      insert_query = """INSERT INTO Opportunities (Id, Name, Amount, CloseDate, OwnerId, StageName, LastModifiedDate, SeOwner, ProductInterest, Competitors)
         VALUES ('%(id)s', '%(name)s', '%(amount)s', '%(close_date)s', '%(owner_id)s', '%(stage_name)s', '%(last_modified)s',
                  '%(se_owner)s', '%(product_interest)s', '%(competitors)s')
         """ % {'id': accounts['id'],
          'name': accounts['name'],
          'amount': accounts['amount'],
          'close_date': accounts['close_date'],
          'owner_id': accounts['owner_id'],
          'stage_name': accounts['stage_name'],
          'last_modified': accounts['last_modified'],
          'se_owner': accounts['se_owner'],
          'product_interest': accounts['product_interest'],
          'competitors': accounts['competitors']
          }
      print insert_query
      Query(insert_query)

   while (str(results[sf.done]) == 'false'):
      results = sf_client.queryMore(str(results[sf.queryLocator]))
      accounts = {}
      for rec in results[sf.records:]:
         accounts['id'] = str(rec[2])
         accounts['name'] = re.escape(str(rec[3]))
         accounts['amount'] = str(rec[4])
         if not accounts['amount']:
            accounts['amount'] = 00.0
         accounts['close_date'] = str(rec[5])
         accounts['owner_id'] = str(rec[6])
         accounts['stage_name'] = str(rec[7])
         soql_date = datetime.datetime.strptime(str(rec[8]),SF_DATETIME)
         accounts['last_modified'] = soql_date.strftime(SQL_DATETIME)
         accounts['se_owner'] = str(rec[9])

         insert_query = """INSERT INTO Opportunities (Id, Name, Amount, CloseDate, OwnerId, StageName, LastModifiedDate, SeOwner)
            VALUES ('%(id)s', '%(name)s', '%(amount)s', '%(close_date)s', '%(owner_id)s', '%(stage_name)s', '%(last_modified)s', '%(se_owner)s')""" % {'id': accounts['id'],
             'name': accounts['name'],
             'amount': accounts['amount'],
             'close_date': accounts['close_date'],
             'owner_id': accounts['owner_id'],
             'stage_name': accounts['stage_name'],
             'last_modified': accounts['last_modified'],
             'se_owner': accounts['se_owner']}
         Query(insert_query)


def InsertOpportunityHistory(sf_client, sf):
   Query("TRUNCATE OpportunityHistory")
   query = "SELECT OpportunityId, OldValue, NewValue, CreatedDate FROM OpportunityFieldHistory WHERE Field='StageName'"
   results = sf_client.query(query)
   opp_records = {}
   for rec in results[sf.records:]:
      opp_records['opportunity_id'] = str(rec[2])
      opp_records['old_value'] = str(rec[3])
      opp_records['new_value'] = str(rec[4])
      soql_date = datetime.datetime.strptime(str(rec[5]),SF_DATETIME)
      opp_records['created_date'] = soql_date.strftime(SQL_DATETIME)
      insert_query = """INSERT INTO OpportunityHistory (OpportunityId, OldValue, NewValue, CreatedDate)
            VALUES ('%(opportunity_id)s', '%(old_value)s', '%(new_value)s', '%(created_date)s')""" % {'opportunity_id': opp_records['opportunity_id'],
            'old_value': opp_records['old_value'],
            'new_value': opp_records['new_value'],
            'created_date': opp_records['created_date']}
      Query(insert_query)

   while (str(results[sf.done]) == 'false'):
      results = sf_client.queryMore(str(results[sf.queryLocator]))
      opp_records = {}
      for rec in results[sf.records:]:
         opp_records['opportunity_id'] = str(rec[2])
         opp_records['old_value'] = str(rec[3])
         opp_records['new_value'] = str(rec[4])
         soql_date = datetime.datetime.strptime(str(rec[5]),SF_DATETIME)
         opp_records['created_date'] = soql_date.strftime(SQL_DATETIME)
         insert_query = """INSERT INTO OpportunityHistory (OpportunityId, OldValue, NewValue, CreatedDate)
               VALUES ('%(opportunity_id)s', '%(old_value)s', '%(new_value)s', '%(created_date)s')""" % {'opportunity_id': opp_records['opportunity_id'],
               'old_value': opp_records['old_value'],
               'new_value': opp_records['new_value'],
               'created_date': opp_records['created_date']}
         Query(insert_query)

def InsertUsers(sf_client, sf):
   Query("TRUNCATE Users")
   query = """select Id, FirstName, LastName, UserName From User WHERE IsActive=True
            AND UserName NOT IN ('albert@kontagent.com',
            'alex.lee@kontagent.com',
            'andrew.kakojejko@kontagent.com',
            'anthony.yoo@kontagent.com',
            'ben.campi@kontagent.com',
            'brett.seyler@kontagent.com',
            'cemile.armas@kontagent.com',
            'chris.thomas@kontagent.com',
            'christine.yang@kontagent.com',
            'jacob.lerman@kontagent.com',
            'joseph.hunt@kontagent.com',
            'justin.davidson@kontagent.com',
            'karl.puzon@kontagent.com',
            'kyle.dipentima@kontagent.com',
            'lowell.alexander@kontagent.com',
            'michael.duvall@kontagent.com',
            'mitch.morando@kontagent.com',
            'nai.saeturn@kontagent.com',
            'raphael.parker@kontagent.com',
            'rob.vanwart@kontagent.com',
            'robert.mitchell@kontagent.com',
            'ryan.tankoos@kontagent.com',
            'steve.brancale@kontagent.com',
            'tj.bonaventura@kontagent.com',
            'vivian.ohanian@kontagent.com',
            'yonatan.lerner@kontagent.com' )
         """
   results = sf_client.query(query)
   users = {}
   for rec in results[sf.records:]:
      users['owner_id'] = str(rec[2])
      users['first_name'] = re.escape(str(rec[3]))
      users['last_name'] = re.escape(str(rec[4]))
      users['username'] = re.escape(str(rec[5]))
      insert_query = """INSERT INTO Users (OwnerId, FirstName, LastName, UserName)
         VALUES ('%(owner_id)s', '%(first_name)s', '%(last_name)s', '%(username)s')""" % {'owner_id': users['owner_id'],
         'first_name': users['first_name'],
         'last_name': users['last_name'],
         'username': users['username']}
      print insert_query
      Query(insert_query)

def create_account(sf_client, sf):
    lead = {'type':'Account', 'Name':'TestCo Inc', 'Type':'Freemium Customer'}
    acct = sf_client.create(lead)
    if str(acct[sf.success]) == 'true':
        new_id = str(acct[sf.id])
    else:
        print 'shit'
    contact = {'type':'Contact', 'AccountId': new_id, 'FirstName': 'Test', 'LastName':'test2', 'Email': 'test@test.com'}
    contact = sf_client.create(contact)
    if str(contact[sf.success]) == 'true':
        print str(contact[sf.id])
    else:
        print 'damn it'



def main():
   sf = beatbox._tPartnerNS
   sf_client = GetSFConn(USERNAME, PASSWORD, SECURITY_TOKEN)
   #InsertOpportunities(sf_client, sf)
   #InsertOpportunityHistory(sf_client, sf)
   #InsertUsers(sf_client, sf)
   create_account(sf_client, sf)

if __name__ == "__main__":
    main()
