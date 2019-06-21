DROP SCHEMA IF EXISTS project1 CASCADE;
CREATE SCHEMA project1;

CREATE TABLE IF NOT EXISTS project1.artist (
        artist_id                       integer,
        artist_name                     varchar(255),
        PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS project1.song (
        song_id                         integer,
        artist_id                       integer,
        song_name                       varchar(255),
        page_link                       varchar(1000),
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) REFERENCES project1.artist(artist_id)
);

CREATE TABLE IF NOT EXISTS project1.token (
        song_id                         integer,
        token                           varchar(255),
        count	                        integer,
        PRIMARY KEY (song_id, token)
);

CREATE TABLE IF NOT EXISTS project1.tfidf (
        song_id                         integer,
        token                           varchar(255),
        score	                        float,
        PRIMARY KEY (song_id, token)
);
