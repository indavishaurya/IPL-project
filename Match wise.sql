SELECT
  f3.*,
  w.Position,
  pp.phase_runrate AS powerplay_runrate,
  pp.avgphasewickets_num AS powerplay_wickets,
  mo.phase_runrate AS middleover_runrate,
  mo.avgphasewickets_num AS middeleover_wickets,
  do.phase_runrate AS deathover_runrate,
  do.avgphasewickets_num AS deathover_wickets
FROM (SELECT
  f2.batting_team AS Team_name,
  f2.season,
  CAST(AVG(CAST(f2.inningwicket_num AS decimal(15, 2))) AS decimal(15, 2)) AS avg_wicket,
  CAST(AVG(CAST(f2.inningnonboundary_runs AS decimal(15, 2))) AS decimal(15, 1)) AS avgnonboundary_runs,
  CAST(AVG(CAST(f2.inningboundary_runs AS decimal(15, 2))) AS decimal(15, 1)) AS avgboundary_runs,
  CAST(AVG(CAST(f2.inningdotballs_num AS decimal(15, 2))) AS decimal(15, 1)) AS avgdotball_num,
  CAST(AVG(CAST(f2.inning_balls AS decimal(15, 2))) AS decimal(15, 1)) AS avginning_balls,
  CAST(AVG(CAST(f2.innings_runs AS decimal(15, 2))) AS decimal(15, 1)) AS avginning_runs,
  SUM(f2.match_toss) AS total_toss,
  COUNT(f2.match_toss) AS total_matches,
  SUM(f2.match_result) AS wins_num
FROM (SELECT DISTINCT
  f1.match_id,
  f1.inning,
  CASE
    WHEN f1.batting_team = 'Rising Pune Supergiant' THEN 'Rising Pune Supergiants'
    ELSE f1.batting_team
  END AS batting_team,
  f1.season,
  SUM(f1.wicket) OVER (PARTITION BY f1.match_id, f1.inning) AS inningwicket_num,
  SUM(f1.four_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningfours_runs,
  SUM(f1.six_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningsix_runs,
  SUM(f1.boundary_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningboundary_runs,
  SUM(f1.nonboundary_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS inningnonboundary_runs,
  SUM(f1.dot_balls) OVER (PARTITION BY f1.match_id, f1.inning) AS inningdotballs_num,
  SUM(f1.over_balls) OVER (PARTITION BY f1.match_id, f1.inning) AS inning_balls,
  SUM(f1.over_runs) OVER (PARTITION BY f1.match_id, f1.inning) AS innings_runs,
  AVG(f1.toss) OVER (PARTITION BY f1.match_id, f1.inning) AS match_toss,
  AVG(f1.result) OVER (PARTITION BY f1.match_id, f1.inning) AS match_result
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
         f.over_category) AS f1) AS f2
GROUP BY f2.season,
         f2.batting_team) AS f3
LEFT JOIN winners AS w
  ON f3.season = w.season
  AND f3.Team_name = w.batting_team
INNER JOIN (SELECT
  CASE
    WHEN batting_team = 'Rising Pune Supergiant' THEN 'Rising Pune Supergiants'
    ELSE batting_team
  END AS batting_team,
  season,
  over_category,
  AVG(phaseballs_num) AS avgphaseballs_num,
  AVG(phaseruns_num) AS avgphaseruns_num,
  CAST(CAST(AVG(phaseruns_num) AS decimal(15, 2)) / CAST(AVG(phaseballs_num) AS decimal(15, 2)) * 6 AS decimal(15, 2)) AS phase_runrate,
  CAST(AVG(CAST(phasewickets_num AS decimal(15, 2))) AS decimal(15, 2)) AS avgphasewickets_num
FROM phasewise_analysis
WHERE over_category = 'powerplay'
GROUP BY batting_team,
         season,
         over_category) AS pp
  ON Team_name = pp.batting_team
  AND f3.season = pp.season
INNER JOIN (SELECT
  CASE
    WHEN batting_team = 'Rising Pune Supergiant' THEN 'Rising Pune Supergiants'
    ELSE batting_team
  END AS batting_team,
  season,
  over_category,
  AVG(phaseballs_num) AS avgphaseballs_num,
  AVG(phaseruns_num) AS avgphaseruns_num,
  CAST(CAST(AVG(phaseruns_num) AS decimal(15, 2)) / CAST(AVG(phaseballs_num) AS decimal(15, 2)) * 6 AS decimal(15, 2)) AS phase_runrate,
  CAST(AVG(CAST(phasewickets_num AS decimal(15, 2))) AS decimal(15, 2)) AS avgphasewickets_num
FROM phasewise_analysis
WHERE over_category = 'middle_overs'
GROUP BY batting_team,
         season,
         over_category) AS mo
  ON f3.Team_name = mo.batting_team
  AND f3.season = mo.season
INNER JOIN (SELECT
  CASE
    WHEN batting_team = 'Rising Pune Supergiant' THEN 'Rising Pune Supergiants'
    ELSE batting_team
  END AS batting_team,
  season,
  over_category,
  AVG(phaseballs_num) AS avgphaseballs_num,
  AVG(phaseruns_num) AS avgphaseruns_num,
  CAST(CAST(AVG(phaseruns_num) AS decimal(15, 2)) / CAST(AVG(phaseballs_num) AS decimal(15, 2)) * 6 AS decimal(15, 2)) AS phase_runrate,
  CAST(AVG(CAST(phasewickets_num AS decimal(15, 2))) AS decimal(15, 2)) AS avgphasewickets_num
FROM phasewise_analysis
WHERE over_category = 'middle_overs'
GROUP BY batting_team,
         season,
         over_category) AS do
  ON f3.Team_name = do.batting_team
  AND f3.season = do.season