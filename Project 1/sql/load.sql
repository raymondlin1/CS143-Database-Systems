CREATE TABLE project1.tfidf AS
SELECT
    song_id, token,
    (count * (log((SELECT COUNT(*) FROM project1.song)::float / dfi)))::float AS score
FROM (
    SELECT L.song_id, L.token, L.count, R.dfi
    FROM (
        SELECT
            token, COUNT(*) as dfi
        FROM project1.token
        GROUP BY token  
    ) R
    JOIN project1.token L
    ON L.token = R.token     
) foo;