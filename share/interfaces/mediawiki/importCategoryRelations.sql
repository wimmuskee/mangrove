DROP TABLE IF EXISTS category_relations;

CREATE TABLE category_relations (
  page_id int(8) unsigned NOT NULL,
  parent_id int(8) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE category_relations ADD INDEX (page_id);
ALTER TABLE category_relations ADD INDEX (parent_id);

INSERT INTO category_relations
SELECT categorylinks.cl_from, categories.id FROM categorylinks 
LEFT JOIN categories ON categorylinks.cl_to = categories.category
WHERE categorylinks.cl_type = 'subcat';


-- DELETE FROM categorylinks WHERE cl_type = 'subcat' OR cl_type = 'file';
-- ALTER TABLE categorylinks DROP INDEX cl_sortkey;
-- ALTER TABLE categorylinks DROP INDEX cl_timestamp;
-- ALTER TABLE categorylinks DROP cl_sortkey;
-- ALTER TABLE categorylinks DROP cl_type;

