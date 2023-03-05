SELECT DISTINCT
  f1.match_id,
  f1.inning,
  f1.batting_team,
  f1.season,
  f1.over_category,
  SUM(f1.over_balls) OVER (PARTITION BY f1.match_id, f1.inning, f1.over_category) AS phaseballs_num,
  SUM(f1.over_runs) OVER (PARTITION BY f1.match_id, f1.inning, f1.over_category) AS phaseruns_num,
  SUM(f1.wicket) OVER (PARTITION BY f1.match_id, f1.inning, f1.over_category) AS phasewickets_num,
  SUM(f1.wicket) OVER (PARTITION BY f1.match_id, f1.inning) AS inningwicket_num,
  SUM(f1.four_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningfours_runs,
  SUM(f1.six_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningsix_runs,
  SUM(f1.boundary_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningboundary_runs,
  SUM(f1.nonboundary_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningnonboundary_runs,
  SUM(f1.dot_balls) OVER (PARTITION BY f1.match_id, f1.inning) AS inningdotballs_num,
  SUM(f1.over_balls) OVER (PARTITION BY f1.match_id, f1.inning) AS inning_balls,
  SUM(f1.over_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS innings_runs,
  AVG(f1.toss) OVER (PARTITION BY f1.match_id, f1.inning) AS match_toss,
  AVG(f1.result) OVER (PARTITION BY f1.match_id, f1.inning) AS match_result INTO phasewise_analysis
FROM (SELECT DISTINCT
  f.match_id,
  f.inning,
  f.batting_team,
  f.[over],
  f.season,
  f.wicket,
  f.toss,
  f.result,
  f.four_runs,
  f.six_runs,
  f.nonboundary_runs,
  (f.four_runs + f.six_runs) AS boundary_runs,
  f.dot_balls,
  f.over_balls,
  f.over_runs,
  f.over_category
FROM (SELECT
  *,
  SUM(CASE
    WHEN player_dismissed = 'N/A' THEN 0
    ELSE 1
  END) OVER (PARTITION BY match_id, inning, [over]) AS wicket,
  CASE
    WHEN toss_winner = batting_team THEN 1
    ELSE 0
  END AS toss,
  CASE
    WHEN winner =
      batting_team THEN 1
    ELSE 0
  END AS result,
  SUM(CASE
    WHEN batsman_runs = 4 THEN 4
    ELSE 0
  END) OVER (PARTITION BY match_id, inning, [over]) AS four_runs,
  SUM(CASE
    WHEN batsman_runs = 6 THEN 6
    ELSE 0
  END) OVER (PARTITION BY match_id, inning, [over]) AS six_runs,
  SUM(CASE
    WHEN batsman_runs <> 4 AND
      batsman_runs <> 6 THEN batsman_runs
    ELSE 0
  END) OVER (PARTITION BY match_id, inning, [over]) AS nonboundary_runs,
  SUM(CASE
    WHEN total_runs = 0 THEN 1
    ELSE 0
  END) OVER (PARTITION BY match_id, inning, [over]) AS dot_balls,
  SUM(legal_ball) OVER (PARTITION BY match_id, inning, [over]) AS over_balls,
  SUM(total_runs) OVER (PARTITION BY match_id, inning, [over]) AS over_runs,
  CASE
    WHEN [over] BETWEEN 1 AND 6 THEN 'powerplay'
    WHEN [over] BETWEEN 7 AND 15 THEN 'middle_overs'
    ELSE 'death_overs'
  END AS over_category
FROM cricket_temp1) AS f
WHERE inning IN (1, 2)
GROUP BY f.match_id,
         f.inning,
         f.batting_team,
         f.[over],
         f.season,
         f.wicket,
         f.toss,
         f.result,
         f.four_runs,
         f.six_runs,
         f.nonboundary_runs,
         f.dot_balls,
         f.over_balls,
         f.over_runs,
         f.over_category) AS f1
ORDER BY f1.match_id, f1.inning, f1.over_category DESC
