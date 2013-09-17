DROP TABLE IF EXISTS categories;

CREATE TABLE categories (
  id int(8) unsigned NOT NULL,
  category varchar(500) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

ALTER TABLE categories ADD INDEX (category);

INSERT INTO categories
SELECT page_id, page_title FROM page 
WHERE page_namespace = 14;


