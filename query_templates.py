
#Pipeline Movements Query
pipeline_movements = """
   SELECT
      DATE_FORMAT(hist.CreatedDate, '%%Y %%U') AS Week,
      SUM(IF(hist.NewValue='Prospecting',1,0)) AS Prospecting,     
      SUM(IF(hist.NewValue='Qualified Opportunity',1,0)) AS Qualified,
      SUM(IF(hist.NewValue='Solution Design',1,0)) AS SolutionDesign,      
      SUM(IF(hist.NewValue='Proposal',1,0)) AS Proposal,
      SUM(IF(hist.NewValue='Contract Review',1,0)) AS ContractReview,
      SUM(IF(hist.NewValue='Pending Closed',1,0)) AS PendingClosed,
      SUM(IF(hist.NewValue='Paid POC',1,0)) AS PaidPOC,
      SUM(IF(hist.NewValue='Closed Won',1,0)) AS Won,
      SUM(IF(hist.NewValue='Closed Lost',1,0)) AS Lost,
      SUM(IF(hist.NewValue='Close Suspended',1,0)) AS Suspended
      
   FROM
      Opportunities opp INNER JOIN OpportunityHistory hist ON (opp.Id=hist.OpportunityId)
   WHERE
      hist.CreatedDate >= '2013-05-01' 

   GROUP BY Week
   ORDER BY Week ASC
   """

#Last Modified Intervals
last_modified_intervals = """
   SELECT 
      usr.UserName  AS User,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 7 DAY),1,0)) AS OneWeeks,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 14 DAY),1,0)) AS TwoWeeks,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 30 DAY) ,1,0)) AS OneMonth,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 60 DAY) ,1,0)) AS TwoMonth
   FROM
      Opportunities opp INNER JOIN Users usr ON (usr.OwnerId=opp.OwnerId)
      INNER JOIN OpportunityHistory hist ON (opp.Id=hist.OpportunityId)

   WHERE
       opp.CloseDate < '%(quarter_end)s'
   GROUP BY
   User
   """
#Last Modified by Stage

last_modified_stage = """
   SELECT 
      opp.StageName  AS Stage,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 7 DAY),1,0)) AS OneWeeks,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 14 DAY),1,0)) AS TwoWeeks,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 30 DAY) ,1,0)) AS OneMonth,
      SUM(IF(opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 60 DAY) ,1,0)) AS TwoMonth

   FROM
      Opportunities opp INNER JOIN Users usr ON (usr.OwnerId=opp.OwnerId)
      INNER JOIN OpportunityHistory hist ON (opp.Id=hist.OpportunityId)

   WHERE
       opp.CloseDate <= '%(quarter_end)s' AND opp.CloseDate > '%(year_start)s' 
       AND opp.StageName NOT IN ('Close Suspended', 'Closed Lost', 'Close Suspended - Intro Call Canceled', 
       'Closed Won')
   GROUP BY
   opp.StageName
   """

#Testing lastmodified
testing_query = """
   SELECT
      opp.Id,
      opp.LastModifiedDate
   FROM
      Opportunities opp INNER JOIN Users usr ON (usr.OwnerId=opp.OwnerId)
      INNER JOIN OpportunityHistory hist ON (opp.Id=hist.OpportunityId)
   WHERE
      opp.LastModifiedDate < DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND FirstName = 'Farlan' AND opp.CloseDate < '%(quarter_end)s'
   """

#Deals by Rep
rep_deals = """
   SELECT
      usr.UserName AS Name,
      SUM(IF(opp.StageName='Prospecting',1,0)) AS Prospecting,     
      SUM(IF(opp.StageName='Qualified Opportunity',1,0)) AS Qualified,
      SUM(IF(opp.StageName='Solution Design',1,0)) AS SolutionDesign,      
      SUM(IF(opp.StageName='Proposal',1,0)) AS Proposal,
      SUM(IF(opp.StageName='Contract Review',1,0)) AS ContractReview,
      SUM(IF(opp.StageName='Pending Closed',1,0)) AS PendingClosed,
      SUM(IF(opp.StageName='Paid POC',1,0)) AS PaidPOC,
      SUM(IF(opp.StageName='Closed Won',1,0)) AS Won,
      SUM(IF(opp.StageName='Closed Lost',1,0)) AS Lost,
      SUM(IF(opp.StageName='Close Suspended',1,0)) AS Suspended
   FROM
      Opportunities opp INNER JOIN Users usr ON (opp.OwnerId=usr.OwnerId)
      
   WHERE
      opp.CloseDate <= '%(quarter_end)s' AND opp.CloseDate > '%(year_start)s'
   GROUP BY
      UserName

   """
opp_details = """
   SELECT
      opp.Id,
      opp.LastModifiedDate,
      opp.Name,
      opp.Amount,
      opp.CloseDate,
      usr.UserName
   FROM
      Opportunities opp INNER JOIN Users usr ON (usr.OwnerId=opp.OwnerId)
   WHERE
      opp.CloseDate <= '%(quarter_end)s' AND opp.CloseDate > '%(year_start)s'
      %(params)s
   """ 

se_details = """
   SELECT
      usr.UserName AS SE,
      opp.SeOwner AS SeOwner,
      COUNT(opp.Id) AS Total,
      SUM(IF(opp.StageName='Prospecting',1,0)) AS Prospecting,     
      SUM(IF(opp.StageName='Qualified Opportunity',1,0)) AS Qualified,
      SUM(IF(opp.StageName='Solution Design',1,0)) AS SolutionDesign,      
      SUM(IF(opp.StageName='Proposal',1,0)) AS Proposal,
      SUM(IF(opp.StageName='Contract Review',1,0)) AS ContractReview,
      SUM(IF(opp.StageName='Pending Closed',1,0)) AS PendingClosed,
      SUM(IF(opp.StageName='Paid POC',1,0)) AS PaidPOC,
      SUM(IF(opp.StageName='Closed Won',1,0)) AS Won,
      SUM(IF(opp.StageName='Closed Lost',1,0)) AS Lost,
      SUM(IF(opp.StageName='Close Suspended',1,0)) AS Suspended

   FROM
      Opportunities opp INNER JOIN Users usr ON (usr.OwnerId=opp.SeOwner)

   WHERE 
      opp.CloseDate <= '%(quarter_end)s' AND opp.CloseDate > '%(year_start)s'
   GROUP BY SE
   """
se_opp_details = """
   SELECT
      opp.Id,
      opp.LastModifiedDate,
      opp.Name,
      opp.Amount,
      opp.CloseDate,
      usr.UserName
   FROM
      Opportunities opp INNER JOIN Users usr ON (usr.OwnerId=opp.SeOwner)
   WHERE
      opp.CloseDate <= '%(quarter_end)s' AND opp.CloseDate > '%(year_start)s'
      %(params)s
   """ 
se_product_competition = """
   SELECT
      SUM(IF(Competitors != '',1,0)) As Competitors,
      SUM(IF(Competitors = '',1,0)) As NoCompetitors,
      SUM(IF(ProductInterest != '',1,0)) As ProductInterest,
      SUM(IF(ProductInterest = '',1,0)) As NoProductInterest,
      SUM(IF(ProductInterest = '',1,0)) / SUM(IF(ProductInterest != '',1,0)) As ProductCompletion,
      SUM(IF(Competitors = '',1,0)) / SUM(IF(Competitors != '',1,0)) CompetitorCompletion
   FROM
      Opportunities opp
   WHERE
      opp.CloseDate <= '%(quarter_end)s' AND opp.CloseDate > '%(year_start)s' AND SeOwner != ''
   """


