use ht_repository;

/*
  drop table if exists source_refs;
*/

CREATE TABLE source_refs (
  id CHAR(36) NOT NULL,
  file_path VARCHAR(255) NOT NULL,
  line_number INT NOT NULL,
  primary key (id)
);
