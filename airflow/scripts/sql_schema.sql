CREATE DATABASE IF NOT EXISTS tmdb_analytics;
USE tmdb_analytics;

-- Dimensões
CREATE TABLE IF NOT EXISTS Dim_Tempo (
  id_tempo INT AUTO_INCREMENT PRIMARY KEY,
  release_date DATE,
  ano INT,
  mes INT,
  trimestre INT,
  decada INT
);

CREATE TABLE IF NOT EXISTS Dim_Idioma (
  id_idioma INT AUTO_INCREMENT PRIMARY KEY,
  nome_idioma VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS Dim_Filme (
  id_filme VARCHAR(100) PRIMARY KEY,
  title VARCHAR(500),
  original_title VARCHAR(500),
  status VARCHAR(100),
  overview TEXT,
  homepage VARCHAR(500),
  imdb_id VARCHAR(50),
  tagline VARCHAR(500),
  backdrop_path VARCHAR(500),
  poster_path VARCHAR(500),
  id_idioma_original INT,
  FOREIGN KEY (id_idioma_original) REFERENCES Dim_Idioma(id_idioma)
);

CREATE TABLE IF NOT EXISTS Dim_Genero (
  id_genero INT PRIMARY KEY,
  nome_genero VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS Dim_Companhia (
  id_companhia INT PRIMARY KEY,
  nome_companhia VARCHAR(400)
);

CREATE TABLE IF NOT EXISTS Dim_Pais (
  id_pais VARCHAR(20) PRIMARY KEY,
  nome_pais VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS Dim_Keyword (
  id_keyword INT PRIMARY KEY,
  palavra_chave VARCHAR(400)
);

-- Fato
CREATE TABLE IF NOT EXISTS Fato_Filme (
  id_fato INT AUTO_INCREMENT PRIMARY KEY,
  id_filme VARCHAR(100),
  id_tempo INT,
  budget BIGINT,
  revenue BIGINT,
  popularity DOUBLE,
  vote_average DOUBLE,
  vote_count INT,
  runtime INT,
  adult BOOLEAN,
  FOREIGN KEY (id_filme) REFERENCES Dim_Filme(id_filme),
  FOREIGN KEY (id_tempo) REFERENCES Dim_Tempo(id_tempo)
);

-- Tabelas de junção
CREATE TABLE IF NOT EXISTS Filme_Genero (id_filme VARCHAR(100), id_genero INT, PRIMARY KEY (id_filme,id_genero));
CREATE TABLE IF NOT EXISTS Filme_Companhia (id_filme VARCHAR(100), id_companhia INT, PRIMARY KEY (id_filme,id_companhia));
CREATE TABLE IF NOT EXISTS Filme_Pais (id_filme VARCHAR(100), id_pais VARCHAR(20), PRIMARY KEY (id_filme,id_pais));
CREATE TABLE IF NOT EXISTS Filme_Keyword (id_filme VARCHAR(100), id_keyword INT, PRIMARY KEY (id_filme,id_keyword));