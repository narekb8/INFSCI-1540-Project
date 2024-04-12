/* Games where the QB earned > 40 fantasy points, these are rare*/
SELECT * FROM `Fact` WHERE points > 40;

/* C.J. Stroud(him) Game list */
SELECT * FROM `Fact` WHERE Pid = 20;

/* Dak Prescott lifetime points */
SELECT * FROM `Lifetime` WHERE pid = 30;

/* Finding out who has the most 40 point games(Lamar Jackson) */
SELECT pid, COUNT(pid) AS forty_point_games FROM `Fact` WHERE points > 40 GROUP BY pid ORDER BY forty_point_games DESC LIMIT 1;

/* Finding the highest average points scored(Josh Allen) */
SELECT pid, AVG(points) AS avg_points FROM `Fact` GROUP BY pid ORDER BY avg_points DESC LIMIT 1;

/* Figuring out which team hinders QB performance the most(which team QBs score on least) (N.O. Saints) */
SELECT * FROM `Vs` ORDER BY points ASC LIMIT 1;