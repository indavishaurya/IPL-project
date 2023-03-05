Select distinct f1.match_id,f1.inning,f1.batting_team,f1.season,f1.over_category,sum(f1.over_balls) over(partition by f1.match_id,f1.inning,f1.over_category) as phaseballs_num,
sum(f1.over_runs) over(partition by f1.match_id,f1.inning,f1.over_category) as phaseruns_num,sum(f1.wicket) over(partition by f1.match_id,f1.inning,f1.over_category) as phasewickets_num,
sum(f1.wicket) over(partition by f1.match_id,f1.inning) as inningwicket_num,sum(f1.four_runs) over(partition by f1.match_id,f1.inning) as inningfours_runs,
sum(f1.six_runs) over(partition by f1.match_id,f1.inning) as inningsix_runs,sum(f1.boundary_runs) over(partition by f1.match_id,f1.inning) as inningboundary_runs,
sum(f1.nonboundary_runs) over(partition by f1.match_id,f1.inning) as inningnonboundary_runs,sum(f1.dot_balls) over(partition by f1.match_id,f1.inning) as inningdotballs_num,
sum(f1.over_balls) over(partition by f1.match_id,f1.inning) as inning_balls,sum(f1.over_runs) over(partition by f1.match_id,f1.inning) as innings_runs,
avg(f1.toss) over(partition by f1.match_id,f1.inning) as match_toss,avg(f1.result) over(partition by f1.match_id,f1.inning) as match_result
into phasewise_analysis
from
(Select distinct f.match_id,f.inning,f.batting_team,f.[over],f.season,f.wicket,f.toss,f.result,f.four_runs,f.six_runs,f.nonboundary_runs,(f.four_runs+f.six_runs) as boundary_runs,f.dot_balls,f.over_balls,f.over_runs,f.over_category
from
(Select  *, sum(case when player_dismissed='N/A' then 0 else 1 end) over (partition by match_id,inning,[over]) as wicket, case when toss_winner=batting_team then 1 else 0 end as toss,case when winner=
batting_team then 1 else 0 end as result,sum(case when batsman_runs=4 then 4 else 0 end) over(partition by match_id,inning,[over]) as four_runs,
sum(case when batsman_runs=6 then 6 else 0 end) over(partition by match_id,inning,[over]) as six_runs,
sum(case when batsman_runs<>4 and batsman_runs<>6 then batsman_runs else 0 end) over(partition by match_id,inning,[over]) as nonboundary_runs,
sum(case when total_runs=0 then 1 else 0 end) over(partition by match_id,inning,[over]) as dot_balls,
SUM(legal_ball) OVER (PARTITION BY match_id, inning, [over]) AS over_balls,
SUM(total_runs) OVER (PARTITION BY match_id, inning, [over]) AS over_runs,
CASE
    WHEN [over] BETWEEN 1 AND 6 THEN 'powerplay'
    WHEN [over] BETWEEN 7 AND 15 THEN 'middle_overs'
    ELSE 'death_overs'
  END AS over_category
from cricket_temp1) as f
where inning in (1,2)
group by f.match_id,f.inning,f.batting_team,f.[over],f.season,f.wicket,f.toss,f.result,f.four_runs,f.six_runs,f.nonboundary_runs,f.dot_balls,f.over_balls,f.over_runs,f.over_category) as f1
order by f1.match_id,f1.inning,f1.over_category desc 

